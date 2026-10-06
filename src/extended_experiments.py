"""Train, ablate and benchmark the revised tweet classification pipeline.

Run from the repository root. Teacher outputs stay private under work/;
only aggregate results and a hashed split manifest are committed.
"""

import argparse
import copy
import hashlib
import json
import os
import platform
import random
import time
from dataclasses import asdict, dataclass
from pathlib import Path

os.environ.setdefault("USE_TF", "0")

import numpy as np
import pandas as pd
import torch
import torch.nn as nn
import torch.nn.functional as F
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (accuracy_score, average_precision_score,
                             balanced_accuracy_score, brier_score_loss,
                             confusion_matrix, f1_score, precision_score,
                             recall_score, roc_auc_score)
from sklearn.model_selection import StratifiedGroupKFold
from sklearn.preprocessing import StandardScaler
from torch_geometric.nn import GATConv, GCNConv, SAGEConv
from tqdm import tqdm
from transformers import AutoModel, AutoTokenizer

from prepare_labels import confidence_gate

EMBEDDER = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
EMBED_REVISION = "e8f8c211226b894fcb81acc59f3b34ba3efd5f42"


@dataclass(frozen=True)
class ModelConfig:
    name: str
    kind: str = "GraphSAGE"
    hidden: int = 96
    layers: int = 2
    filtered: bool = False
    rich_features: bool = True
    author_weight: float = 0.3
    no_edges: bool = False


CONFIGS = [
    ModelConfig("SAGE-TFIDF", rich_features=False),
    ModelConfig("GCN-small", kind="GCN"),
    ModelConfig("SAGE-small"),
    ModelConfig("GAT-small", kind="GAT"),
    ModelConfig("SAGE-filtered", filtered=True),
    ModelConfig("SAGE-wide", hidden=192, filtered=True),
    ModelConfig("SAGE-deep", hidden=192, layers=4, filtered=True),
    ModelConfig("MLP-control", kind="MLP", hidden=192, layers=4, filtered=True),
    ModelConfig("SAGE-no-edges", hidden=192, layers=4, filtered=True, no_edges=True),
    ModelConfig("SAGE-single-task", hidden=192, layers=4, filtered=True, author_weight=0),
]


def save_json(path, value):
    """Write a machine-readable artifact and create its containing directory."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, allow_nan=False), encoding="utf-8")
    temporary.replace(path)


def seed_everything(seed):
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(seed)


def synchronize(device):
    if device.type == "cuda":
        torch.cuda.synchronize(device)


def make_split(df):
    """Keep equal normalized text in one partition; stratify author and target."""
    ids = np.flatnonzero(df.eligible.to_numpy())
    strata = df.author.astype(str) + ":" + df.target.astype(str)
    groups = df.clean_text.str.casefold().to_numpy()
    outer = StratifiedGroupKFold(5, shuffle=True, random_state=42)
    train_val_pos, test_pos = next(outer.split(ids, strata.iloc[ids], groups[ids]))
    train_val, test = ids[train_val_pos], ids[test_pos]
    inner = StratifiedGroupKFold(5, shuffle=True, random_state=43)
    train_pos, val_pos = next(inner.split(train_val, strata.iloc[train_val], groups[train_val]))
    split = {"train": train_val[train_pos], "val": train_val[val_pos], "test": test}
    sets = [set(groups[split[k]]) for k in split]
    if any(sets[i] & sets[j] for i in range(3) for j in range(i)):
        raise AssertionError("Duplicate text leaked across partitions")
    return split


def build_graph(df):
    """Only retweeter relations, with unique edges; reject every own-author edge."""
    retweeters = [[s.strip() for s in str(v).split(",")
                  if s.strip() and s.strip().lower() != "none"]
                 for v in df.retweeters.fillna("")]
    users = sorted(set(df.author) | {u for row in retweeters for u in row})
    user_id = {u: len(df) + i for i, u in enumerate(users)}
    pairs = set()
    excluded = 0
    for i, row in enumerate(retweeters):
        for user in row:
            if user == df.author.iloc[i]:
                excluded += 1
                continue
            pairs.add((i, user_id[user]))
            pairs.add((user_id[user], i))
    edge = torch.tensor(sorted(pairs), dtype=torch.long).t().contiguous()
    authors = np.array([user_id[a] for a in df.author])
    source, target = edge.numpy()
    forward = source < len(df)
    reverse = ~forward
    residual = int((target[forward] == authors[source[forward]]).sum() +
                   (source[reverse] == authors[target[reverse]]).sum())
    assert residual == 0
    degree = np.bincount(source, minlength=len(df) + len(users))
    return edge, len(df) + len(users), {
        "tweets": len(df), "accounts": len(users), "directed_edges": edge.shape[1],
        "residual_authorship_edges": residual, "self_retweeter_relations_excluded": excluded,
        "tweet_nodes_without_retweeter_edges": int((degree[:len(df)] == 0).sum()),
    }


def sentence_embeddings(df, cache, device, batch_size=32):
    """Frozen multilingual representations; mean pool according to the model card."""
    digest = hashlib.sha256("\n".join(df.clean_text).encode()).hexdigest()
    path = Path(cache) / f"embeddings-{digest[:16]}.npz"
    if path.exists():
        values = np.load(path)
        if str(values["revision"]) != EMBED_REVISION:
            raise ValueError("Embedding cache revision mismatch")
        return values["embeddings"], {"cached": True, "seconds": None,
                                      "model": EMBEDDER, "revision": EMBED_REVISION}
    start = time.perf_counter()
    tokenizer = AutoTokenizer.from_pretrained(EMBEDDER, revision=EMBED_REVISION)
    model = AutoModel.from_pretrained(EMBEDDER, revision=EMBED_REVISION).eval().to(device)
    pieces = []
    for i in tqdm(range(0, len(df), batch_size), desc="Frozen text embeddings"):
        inputs = tokenizer(df.clean_text.iloc[i:i + batch_size].tolist(), padding=True,
                           truncation=True, max_length=128, return_tensors="pt").to(device)
        with torch.inference_mode():
            tokens = model(**inputs).last_hidden_state
            mask = inputs["attention_mask"].unsqueeze(-1)
            pooled = (tokens * mask).sum(1) / mask.sum(1).clamp(min=1)
            pieces.append(F.normalize(pooled, dim=1).cpu().numpy())
    embeddings = np.concatenate(pieces).astype(np.float32)
    path.parent.mkdir(parents=True, exist_ok=True)
    np.savez_compressed(path, embeddings=embeddings, revision=EMBED_REVISION)
    synchronize(device)
    return embeddings, {"cached": False, "seconds": time.perf_counter() - start,
                         "model": EMBEDDER, "revision": EMBED_REVISION}


def prepare_features(df, split, n_nodes, embeddings):
    """Both the vocabulary and activity scaler are fitted on training rows only."""
    vectorizer = TfidfVectorizer(max_features=256, min_df=2, sublinear_tf=True)
    vectorizer.fit(df.clean_text.iloc[split["train"]])
    text = vectorizer.transform(df.clean_text).toarray().astype(np.float32)
    columns = ["tweet.retweet_count", "tweet.reply_count", "tweet.favorite_count"]
    activity = np.log1p(df[columns].apply(pd.to_numeric, errors="coerce").fillna(0).clip(lower=0))
    scaler = StandardScaler().fit(activity.iloc[split["train"]])
    numeric = scaler.transform(activity).astype(np.float32)
    base = np.hstack([text, numeric])
    rich = np.hstack([text, embeddings, numeric])
    def pad(x):
        result = torch.zeros((n_nodes, x.shape[1]), dtype=torch.float32)
        result[:len(df)] = torch.from_numpy(x)
        return result
    return pad(base), pad(rich)


class ResidualNetwork(nn.Module):
    """A projected residual encoder with two separate prediction heads."""

    def __init__(self, in_dim, n_authors, config):
        super().__init__()
        self.config = config
        self.project = nn.Linear(in_dim, config.hidden)
        self.convs = nn.ModuleList()
        self.norms = nn.ModuleList()
        for _ in range(config.layers):
            if config.kind == "GCN":
                layer = GCNConv(config.hidden, config.hidden)
            elif config.kind == "GraphSAGE":
                layer = SAGEConv(config.hidden, config.hidden)
            elif config.kind == "GAT":
                layer = GATConv(config.hidden, config.hidden, heads=2, concat=False, dropout=0.15)
            elif config.kind == "MLP":
                layer = nn.Linear(config.hidden, config.hidden)
            else:
                raise ValueError(config.kind)
            self.convs.append(layer)
            self.norms.append(nn.LayerNorm(config.hidden))
        self.author_head = nn.Linear(config.hidden, n_authors)
        self.label_head = nn.Linear(config.hidden, 2)

    def forward(self, x, edge_index):
        h = F.relu(self.project(x))
        for conv, norm in zip(self.convs, self.norms):
            update = conv(h) if self.config.kind == "MLP" else conv(h, edge_index)
            h = F.dropout(F.relu(norm(update + h)), p=0.25, training=self.training)
        return self.author_head(h), self.label_head(h)


def choose_threshold(truth, probabilities):
    """Use validation labels only; ties favor the threshold closest to 0.5."""
    candidates = sorted(np.arange(0.10, 0.901, 0.05), key=lambda x: abs(x - 0.5))
    scores = [f1_score(truth, probabilities >= t, average="macro", labels=[0, 1], zero_division=0)
              for t in candidates]
    best = int(np.argmax(scores))
    return float(candidates[best]), float(scores[best])


def binary_metrics(truth, probability, threshold):
    """Report the operating point alongside ranking and probability metrics."""
    predicted = probability >= threshold
    return {
        "accuracy": float(accuracy_score(truth, predicted)),
        "macro_f1": float(f1_score(truth, predicted, average="macro", labels=[0, 1], zero_division=0)),
        "balanced_accuracy": float(balanced_accuracy_score(truth, predicted)),
        "positive_precision": float(precision_score(truth, predicted, zero_division=0)),
        "positive_recall": float(recall_score(truth, predicted, zero_division=0)),
        "average_precision": float(average_precision_score(truth, probability)),
        "roc_auc": float(roc_auc_score(truth, probability)) if len(np.unique(truth)) == 2 else None,
        "brier": float(brier_score_loss(truth, probability)),
        "confusion_matrix": confusion_matrix(truth, predicted, labels=[0, 1]).tolist(),
        "n": len(truth), "positive_n": int(np.sum(truth)),
    }


def fit_model(config, x, edges, y_author, y_label, split, df, seed, epochs, device,
              private_dir, output_dir):
    """Select checkpoints and thresholds on validation data, then open the test set."""
    seed_everything(seed)
    x, edges = x.to(device), edges.to(device)
    original_degree = np.bincount(edges[0].cpu(), minlength=len(x))
    if config.no_edges:
        edges = torch.empty((2, 0), dtype=torch.long, device=device)
    ya, ym = y_author.to(device), y_label.to(device)
    train_ids = split["train"]
    if config.filtered:
        train_ids = train_ids[df.accepted.iloc[train_ids].to_numpy()]
    if len(np.unique(y_label[train_ids].numpy())) != 2:
        raise ValueError(f"{config.name}: training gate removed a complete class")
    tr = torch.tensor(train_ids, device=device)
    author_train = torch.tensor(split["train"], device=device)
    va, te = [torch.tensor(split[k], device=device) for k in ("val", "test")]
    sample_weight = torch.ones(len(tr), device=device)
    if config.filtered:
        sample_weight = torch.tensor(df.confidence.iloc[train_ids].to_numpy(),
                                     dtype=torch.float32, device=device)
    counts = torch.bincount(ym[tr], minlength=2).float()
    class_weight = (counts.sum() / (2 * counts)).to(device)
    model = ResidualNetwork(x.shape[1], int(ya.max()) + 1, config).to(device)
    optimizer = torch.optim.AdamW(model.parameters(), lr=0.002, weight_decay=1e-4)
    best, best_score, best_threshold, stalled = None, -1, 0.5, 0
    history = []
    synchronize(device)
    start = time.perf_counter()
    for epoch in tqdm(range(1, epochs + 1), desc=f"{config.name} seed {seed}", leave=False):
        model.train()
        optimizer.zero_grad(set_to_none=True)
        author, label = model(x, edges)
        label_loss = F.cross_entropy(label[tr], ym[tr], weight=class_weight, reduction="none")
        loss = (label_loss * sample_weight).sum() / sample_weight.sum()
        if config.author_weight:
            loss = loss + config.author_weight * F.cross_entropy(author[author_train], ya[author_train])
        loss.backward()
        nn.utils.clip_grad_norm_(model.parameters(), 2.0)
        optimizer.step()
        model.eval()
        with torch.inference_mode():
            author, label = model(x, edges)
            probs = label[va].softmax(1)[:, 1].cpu().numpy()
            threshold, score = choose_threshold(ym[va].cpu().numpy(), probs)
            author_f1 = float(f1_score(ya[va].cpu(), author[va].argmax(1).cpu(), average="macro"))
        history.append({"epoch": epoch, "loss": float(loss.item()),
                        "val_label_macro_f1": score, "val_author_macro_f1": author_f1,
                        "threshold": threshold})
        if score > best_score + 1e-6:
            best = copy.deepcopy(model.state_dict())
            best_score, best_threshold, stalled = score, threshold, 0
        else:
            stalled += 1
        if stalled >= 12:
            break
    synchronize(device)
    train_seconds = time.perf_counter() - start
    model.load_state_dict(best)
    model.eval()
    with torch.inference_mode():
        author, label = model(x, edges)
        probability = label[te].softmax(1)[:, 1].cpu().numpy()
        pred_author = author[te].argmax(1).cpu().numpy()
    truth = ym[te].cpu().numpy()
    result = {
        "config": asdict(config), "seed": seed,
        "parameters": sum(p.numel() for p in model.parameters()),
        "epochs_ran": epoch, "best_epoch": int(np.argmax([h["val_label_macro_f1"] for h in history])) + 1,
        "training_seconds": train_seconds, "training_labels": len(tr),
        "validation_macro_f1": best_score, "threshold": best_threshold,
        "label": binary_metrics(truth, probability, best_threshold),
        "label_default_threshold": binary_metrics(truth, probability, 0.5),
        "author_macro_f1": float(f1_score(ya[te].cpu(), pred_author, average="macro")),
        "history": history,
    }
    for group, mask in {
        "accepted": df.accepted.iloc[split["test"]].to_numpy(),
        "abstained": ~df.accepted.iloc[split["test"]].to_numpy(),
        "no_retweeter": original_degree[split["test"]] == 0,
    }.items():
        if mask.any():
            result[f"test_{group}"] = binary_metrics(truth[mask], probability[mask], best_threshold)
    private_dir = Path(private_dir)
    private_dir.mkdir(parents=True, exist_ok=True)
    np.savez_compressed(private_dir / f"{config.name}-{seed}.npz", ids=split["test"],
                        truth=truth, probability=probability, author_prediction=pred_author)
    torch.save(best, private_dir / f"{config.name}-{seed}.pt")
    save_json(Path(output_dir) / "runs" / f"{config.name}-{seed}.json", result)
    print(f"{config.name} seed={seed}: F1={result['label']['macro_f1']:.3f}, "
          f"author={result['author_macro_f1']:.3f}, {train_seconds:.1f}s", flush=True)
    return result


def text_baselines(df, split):
    """Fit a transparent text predictor with the same partitions and gate choices."""
    results = {}
    target = df.target.to_numpy()
    for filtered in (False, True):
        tr = split["train"]
        if filtered:
            tr = tr[df.accepted.iloc[tr].to_numpy()]
        vectorizer = TfidfVectorizer(ngram_range=(1, 2), max_features=30000,
                                     min_df=2, sublinear_tf=True)
        # Always fit the vocabulary on the same entire training partition.
        vectorizer.fit(df.clean_text.iloc[split["train"]])
        features = vectorizer.transform(df.clean_text)
        clf = LogisticRegression(class_weight="balanced", max_iter=1500, random_state=42)
        clf.fit(features[tr], target[tr])
        threshold, val_score = choose_threshold(target[split["val"]],
                          clf.predict_proba(features[split["val"]])[:, 1])
        probability = clf.predict_proba(features[split["test"]])[:, 1]
        results["Text-LR-filtered" if filtered else "Text-LR"] = {
            "label": binary_metrics(target[split["test"]], probability, threshold),
            "threshold": threshold, "validation_macro_f1": val_score, "training_labels": len(tr)}
    majority = int(np.bincount(target[split["train"]]).argmax())
    results["Majority"] = {"label": binary_metrics(target[split["test"]],
                              np.full(len(split["test"]), majority, dtype=float), 0.5)}
    return results


def induce_subgraph(x, edges, tweet_ids, n_tweets):
    """Subset real tweets plus their account neighbors; do not replicate data."""
    selected = torch.zeros(len(x), dtype=torch.bool)
    selected[tweet_ids] = True
    first = selected[edges[0]] | selected[edges[1]]
    neighbor = torch.unique(edges[:, first])
    selected[neighbor[neighbor >= n_tweets]] = True
    if len(tweet_ids) == n_tweets:
        selected[n_tweets:] = True  # preserve isolated account nodes at full scale
    keep = selected[edges[0]] & selected[edges[1]]
    nodes = torch.cat([torch.as_tensor(tweet_ids), torch.nonzero(selected[n_tweets:]).flatten() + n_tweets])
    remap = torch.full((len(x),), -1, dtype=torch.long)
    remap[nodes] = torch.arange(len(nodes))
    return x[nodes], remap[edges[:, keep]], nodes


def benchmark_scaling(df, x, edges, y_author, y_label, device, repeats=7):
    """Time synchronized warm optimizer steps and inference on real subgraphs."""
    rows = []
    rng = np.random.default_rng(42)
    permutation = rng.permutation(len(df))
    configurations = [c for c in CONFIGS if c.name in ("SAGE-small", "SAGE-wide", "SAGE-deep", "GAT-small")]
    for fraction in (0.125, 0.25, 0.5, 1.0):
        ids = np.sort(permutation[:int(round(len(df) * fraction))])
        sub_x, sub_edges, _ = induce_subgraph(x, edges, ids, len(df))
        eligible = df.eligible.iloc[ids].to_numpy()
        loss_ids = torch.from_numpy(np.flatnonzero(eligible)).to(device)
        sx, se = sub_x.to(device), sub_edges.to(device)
        sy = y_label[ids].to(device)
        sa = y_author[ids].to(device)
        for config in configurations:
            seed_everything(42)
            model = ResidualNetwork(x.shape[1], int(y_author.max()) + 1, config).to(device)
            optimizer = torch.optim.AdamW(model.parameters(), lr=0.002)
            def step():
                model.train()
                optimizer.zero_grad(set_to_none=True)
                a, b = model(sx, se)
                loss = F.cross_entropy(b[loss_ids], sy[loss_ids]) + 0.3 * F.cross_entropy(a[loss_ids], sa[loss_ids])
                loss.backward()
                optimizer.step()
            synchronize(device)
            start = time.perf_counter()
            step()
            synchronize(device)
            cold = time.perf_counter() - start
            step()
            step()
            train_times, inference_times = [], []
            for _ in range(repeats):
                synchronize(device)
                start = time.perf_counter()
                step()
                synchronize(device)
                train_times.append(time.perf_counter() - start)
            model.eval()
            for _ in range(repeats):
                synchronize(device)
                start = time.perf_counter()
                with torch.inference_mode():
                    model(sx, se)
                synchronize(device)
                inference_times.append(time.perf_counter() - start)
            row = {"model": config.name, "fraction": fraction, "tweets": len(ids),
                   "nodes": len(sx), "edges": se.shape[1],
                   "parameters": sum(p.numel() for p in model.parameters()),
                   "first_step_seconds": cold, "train_step_seconds": train_times,
                   "inference_seconds": inference_times,
                   "train_median_seconds": float(np.median(train_times)),
                   "train_iqr_seconds": float(np.percentile(train_times, 75) - np.percentile(train_times, 25)),
                   "inference_median_seconds": float(np.median(inference_times))}
            rows.append(row)
            print(f"Scaling {config.name} tweets={len(ids)} edges={se.shape[1]}: "
                  f"{row['train_median_seconds']:.3f}s/step", flush=True)
    return rows


def summarize(runs):
    summary = {}
    for name in sorted({r["config"]["name"] for r in runs}):
        selected = [r for r in runs if r["config"]["name"] == name]
        metrics = {
            "label_macro_f1": [r["label"]["macro_f1"] for r in selected],
            "author_macro_f1": [r["author_macro_f1"] for r in selected],
            "positive_precision": [r["label"]["positive_precision"] for r in selected],
            "positive_recall": [r["label"]["positive_recall"] for r in selected],
            "average_precision": [r["label"]["average_precision"] for r in selected],
            "training_seconds": [r["training_seconds"] for r in selected],
        }
        summary[name] = {k: {"mean": float(np.mean(v)),
                            "sample_std": float(np.std(v, ddof=1)) if len(v) > 1 else 0.0}
                         for k, v in metrics.items()}
        summary[name].update({"parameters": selected[0]["parameters"],
                             "training_labels": selected[0]["training_labels"],
                             "seeds": [r["seed"] for r in selected]})
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--labels", required=True)
    parser.add_argument("--out", default="results/revision-v2")
    parser.add_argument("--cache", default="work/experiment-cache")
    parser.add_argument("--epochs", type=int, default=70)
    parser.add_argument("--seeds", nargs="+", type=int, default=[42, 43, 44])
    parser.add_argument("--models", nargs="+")
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--skip-scaling", action="store_true")
    args = parser.parse_args()
    torch.set_num_threads(args.threads)
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    print(f"Device={device}; threads={args.threads}", flush=True)
    df = pd.read_csv(args.labels, dtype={"tweet_id": str})
    for c in ("eligible", "accepted", "prompt_agreement"):
        if df[c].dtype != bool:
            raise ValueError(f"Expected boolean {c} column")
    df.clean_text = df.clean_text.fillna("")
    split = make_split(df)
    edges, n_nodes, audit = build_graph(df)
    embeddings, embedding_meta = sentence_embeddings(df, args.cache, device)
    base, rich = prepare_features(df, split, n_nodes, embeddings)
    authors = sorted(df.author.unique())
    author_lookup = {a: i for i, a in enumerate(authors)}
    ya = torch.tensor(df.author.map(author_lookup).to_numpy(), dtype=torch.long)
    ym = torch.tensor(df.target.to_numpy(), dtype=torch.long)
    out = Path(args.out)
    private = Path(args.cache) / out.name / "predictions"
    manifest = {
        "label_csv_sha256": hashlib.sha256(Path(args.labels).read_bytes()).hexdigest(),
        "python": platform.python_version(), "torch": torch.__version__,
        "torch_geometric": __import__("torch_geometric").__version__,
        "transformers": __import__("transformers").__version__,
        "numpy": np.__version__, "sklearn": __import__("sklearn").__version__,
        "platform": platform.platform(), "device": str(device), "threads": args.threads,
        "cpu": platform.processor(), "embedding": embedding_meta, "graph": audit,
        "experiment_source_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "feature_dimensions": {"base": base.shape[1], "rich": rich.shape[1]},
        "split": {k: {"n": len(v), "positive": int(ym[v].sum()),
                       "accepted": int(df.accepted.iloc[v].sum()),
                       "row_indices": v.tolist()} for k, v in split.items()},
        "eligible": int(df.eligible.sum()), "ineligible": int((~df.eligible).sum()),
        "accepted": int(df.accepted.sum()),
        "grouping": "casefolded cleaned text; author+binary stratification; outer42/inner43",
        "epochs": args.epochs, "seeds": args.seeds,
        "selection": "validation binary macro-F1; threshold grid 0.10..0.90 step0.05",
        "label_note": "Teacher agreement, not verified misinformation; new target v2."}
    existing_manifest = out / "manifest.json"
    if existing_manifest.exists():
        old = json.loads(existing_manifest.read_text())
        for key in ("label_csv_sha256", "feature_dimensions", "split", "epochs", "seeds", "experiment_source_sha256"):
            if old[key] != manifest[key]:
                raise ValueError(f"Result directory mismatch at {key}; choose a new --out")
    save_json(existing_manifest, manifest)
    baselines = text_baselines(df, split)
    save_json(out / "baselines.json", baselines)
    configs = [c for c in CONFIGS if not args.models or c.name in args.models]
    if args.models and len(configs) != len(set(args.models)):
        raise ValueError("Unknown or repeated --models entry")
    runs = []
    for config in configs:
        for seed in args.seeds:
            path = out / "runs" / f"{config.name}-{seed}.json"
            if path.exists():
                result = json.loads(path.read_text())
                if result["config"] != asdict(config):
                    raise ValueError(f"Run configuration changed: {path}")
            else:
                result = fit_model(config, rich if config.rich_features else base,
                                   edges, ya, ym, split, df, seed, args.epochs, device,
                                   private, out)
            runs.append(result)
            save_json(out / "summary.json", summarize(runs))
    if not args.skip_scaling and not (out / "scaling.json").exists():
        save_json(out / "scaling.json", benchmark_scaling(df, rich, edges, ya, ym, device))
    print(json.dumps(summarize(runs), indent=2), flush=True)


if __name__ == "__main__":
    main()
