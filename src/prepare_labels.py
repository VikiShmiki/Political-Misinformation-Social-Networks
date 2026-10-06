"""Resumable multilingual pseudo-labeling; no claim of factual verification."""

import argparse
import hashlib
import json
import os
import re
import time
from pathlib import Path
from zipfile import ZipFile

os.environ.setdefault("USE_TF", "0")

import numpy as np
import pandas as pd
import torch
from tqdm import tqdm
from transformers import AutoModelForSequenceClassification, AutoTokenizer

MODEL_ID = "MoritzLaurer/mDeBERTa-v3-base-mnli-xnli"
MODEL_REVISION = "8adb042d524ecd5c26d3e3ba0e3fbcf7e2d0864c"
VIEWS = [
    ["This example is valid.", "This example is misleading.", "This example is invalid."],
    ["This text contains a credible factual statement.",
     "This text contains a misleading statement.",
     "This text contains a false factual statement."],
]
CLASS_NAMES = np.array(["valid", "misleading", "invalid"])


def read_archive(archive):
    """Preserve tweet IDs as strings and give archive rows a stable order."""
    with ZipFile(archive) as zf:
        names = sorted(n for n in zf.namelist() if n.endswith(".csv"))
        frames = [pd.read_csv(zf.open(n), dtype={"tweet_id": str},
                              on_bad_lines="error") for n in names]
    df = pd.concat(frames, ignore_index=True).drop_duplicates("tweet_id").reset_index(drop=True)
    df["text"] = df["tweet.full_text"].fillna("").astype(str)
    df["author"] = df["tweet.user.screen_name"].fillna("unknown").astype(str)
    df["clean_text"] = df["text"].map(clean_text)
    df["eligible"] = df["clean_text"].str.findall(r"\w+").str.len().ge(5)
    return df


def clean_text(text):
    """Remove links/handles and normalize whitespace without translating text."""
    text = re.sub(r"https?://\S+|@\w+", " ", str(text))
    return re.sub(r"\s+", " ", text).strip()


def confidence_gate(scores, eligible, min_confidence=0.75, min_margin=0.15):
    """Accept binary prompt agreement with a confident three-way decision."""
    scores = np.asarray(scores)
    binary_positive = scores[:, :, 1:].sum(-1)
    binary_votes = scores.argmax(-1) != 0
    chosen_probability = np.where(binary_votes, binary_positive, 1 - binary_positive)
    confidence = chosen_probability.min(axis=1)
    ranked = np.sort(scores, axis=-1)
    margin = (ranked[:, :, -1] - ranked[:, :, -2]).min(axis=1)
    agreement = binary_votes[:, 0] == binary_votes[:, 1]
    accepted = np.asarray(eligible) & agreement & (confidence >= min_confidence) & (margin >= min_margin)
    return accepted, agreement, confidence, margin


def score_pairs(model, tokenizer, texts, hypotheses, batch_size, max_length, device):
    """Normalize entailment logits across candidates as in single-label NLI."""
    entailment = [int(k) for k, v in model.config.id2label.items() if "entail" in v.lower()]
    if len(entailment) != 1:
        raise ValueError(f"Cannot identify entailment label: {model.config.id2label}")
    premises = [t for t in texts for _ in hypotheses]
    hypothesis_pairs = hypotheses * len(texts)
    logits = []
    for start in range(0, len(premises), batch_size):
        tokens = tokenizer(premises[start:start + batch_size],
                           hypothesis_pairs[start:start + batch_size],
                           padding=True, truncation="only_first", max_length=max_length,
                           return_tensors="pt").to(device)
        with torch.inference_mode():
            logits.append(model(**tokens).logits[:, entailment[0]].float().cpu())
    return torch.cat(logits).reshape(-1, 3).softmax(-1).numpy()


def materialize_labels(df, scores):
    """Store missing evidence explicitly, never as a forced valid prediction."""
    accepted, agreement, confidence, margin = confidence_gate(scores, df.eligible)
    df = df.copy()
    df["misinformation_label"] = CLASS_NAMES[scores[:, 0].argmax(-1)]
    df["descriptive_label"] = CLASS_NAMES[scores[:, 1].argmax(-1)]
    df["target"] = (scores[:, 0].argmax(-1) != 0).astype(int)
    df["accepted"] = accepted
    df["prompt_agreement"] = agreement
    df["confidence"] = confidence
    df["margin"] = margin
    missing = ~df.eligible
    df.loc[missing, ["misinformation_label", "descriptive_label"]] = "unlabeled"
    df.loc[missing, "target"] = -1
    df.loc[missing, ["confidence", "margin"]] = 0.0
    df.loc[missing, "prompt_agreement"] = False
    return df


def label_summary(df):
    return {"eligible": int(df.eligible.sum()), "accepted": int(df.accepted.sum()),
            "binary_counts_eligible": df.loc[df.eligible, "target"].value_counts().to_dict(),
            "binary_counts_accepted": df.loc[df.accepted, "target"].value_counts().to_dict(),
            "prompt_agreement_eligible": float(df.loc[df.eligible, "prompt_agreement"].mean()),
            "missing_evidence_policy": "unlabeled; target=-1; excluded from every supervised loss"}


def save_scores(path, scores):
    """Atomically replace checkpoints, preserving the prior chunk on interruption."""
    temporary = path.with_name(path.stem + ".tmp.npz")
    np.savez_compressed(temporary, scores=scores)
    temporary.replace(path)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--model", default=MODEL_ID)
    parser.add_argument("--revision", default=MODEL_REVISION)
    parser.add_argument("--batch-size", type=int, default=12, help="NLI pairs per model batch")
    parser.add_argument("--max-length", type=int, default=128)
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--cached-only", action="store_true", help="Materialize complete scores without loading the teacher")
    args = parser.parse_args()
    torch.set_num_threads(args.threads)
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    df = read_archive(args.archive)
    if args.limit:
        df = df.iloc[:args.limit].copy()
    archive_sha = hashlib.sha256(Path(args.archive).read_bytes()).hexdigest()
    signature = dict(archive_sha256=archive_sha, model=args.model, revision=args.revision,
                     max_length=args.max_length, dtype="float32", views=VIEWS, rows=len(df))
    score_path = out / "teacher_scores.npz"
    manifest_path = out / "label_manifest.json"
    if manifest_path.exists():
        old = json.loads(manifest_path.read_text())
        if old["signature"] != signature:
            raise ValueError("Cache configuration differs; choose a new --out directory")
    if args.cached_only:
        if not score_path.exists() or not manifest_path.exists():
            raise FileNotFoundError("Complete score cache and manifest are required")
        scores = np.load(score_path)["scores"]
        if np.isnan(scores[df.eligible]).any():
            raise ValueError("Teacher score cache is incomplete")
        df = materialize_labels(df, scores)
        df.to_csv(out / "labeled_tweets.csv", index=False)
        old.update(label_summary(df))
        manifest_path.write_text(json.dumps(old, indent=2), encoding="utf-8")
        print(json.dumps(old, indent=2), flush=True)
        return
    tokenizer = AutoTokenizer.from_pretrained(args.model, revision=args.revision)
    model = AutoModelForSequenceClassification.from_pretrained(args.model, revision=args.revision)
    resolved_revision = getattr(model.config, "_commit_hash", None)
    model.float().eval().to(device)
    # Reject an incompatible/broken model before assigning thousands of labels.
    sanity = tokenizer(["A dog is running in the park."] * 3,
                       ["An animal is outdoors.", "No animal is outdoors.", "The dog is brown."],
                       padding=True, return_tensors="pt").to(device)
    with torch.inference_mode():
        sanity_logits = model(**sanity).logits
    expected = ["entailment", "contradiction", "neutral"]
    observed = [model.config.id2label[i] for i in sanity_logits.argmax(-1).tolist()]
    if observed != expected:
        raise ValueError(f"NLI sanity check failed: {observed} != {expected}")
    scores = np.full((len(df), 2, 3), np.nan, dtype=np.float32)
    if score_path.exists():
        scores[:] = np.load(score_path)["scores"]
    manifest = {"signature": signature, "resolved_revision": resolved_revision,
                "device": str(device), "threads": args.threads,
                "note": "Pseudo-label confidence is not calibrated factual correctness."}
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    indices = np.flatnonzero(df.eligible.to_numpy())
    started = time.perf_counter()
    # Save after each small chunk, so a disconnected session does not lose work.
    for view, hypotheses in enumerate(VIEWS):
        todo = indices[np.isnan(scores[indices, view, 0])]
        for start in tqdm(range(0, len(todo), 32), desc=f"Label view {view + 1}/2"):
            chosen = todo[start:start + 32]
            scores[chosen, view] = score_pairs(model, tokenizer,
                df.clean_text.iloc[chosen].tolist(), hypotheses,
                args.batch_size, args.max_length, device)
            save_scores(score_path, scores)
    # Missing evidence is explicit; uniform scores are placeholders, never accepted.
    scores[~df.eligible.to_numpy()] = 1 / 3
    save_scores(score_path, scores)
    df = materialize_labels(df, scores)
    df.to_csv(out / "labeled_tweets.csv", index=False)
    manifest.update(label_summary(df))
    manifest["elapsed_this_invocation_seconds"] = time.perf_counter() - started
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(json.dumps(manifest, indent=2), flush=True)


if __name__ == "__main__":
    main()
