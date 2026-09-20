"""Multi-task GNN experiment for political tweets."""

import copy
import random
import time

import numpy as np
import torch
import torch.nn as nn
import torch.nn.functional as F
from sklearn.metrics import accuracy_score, f1_score
from sklearn.dummy import DummyClassifier
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import balanced_accuracy_score, confusion_matrix
from sklearn.model_selection import train_test_split
from torch_geometric.nn import GATConv, GCNConv, SAGEConv


class MultiTaskGNN(nn.Module):
    """Shared graph encoder with author and misinformation prediction heads."""

    def __init__(self, kind, in_dim, hidden_dim, n_authors):
        super().__init__()
        if kind == "GCN":
            self.conv1, self.conv2 = GCNConv(in_dim, hidden_dim), GCNConv(hidden_dim, hidden_dim)
        elif kind == "GraphSAGE":
            self.conv1, self.conv2 = SAGEConv(in_dim, hidden_dim), SAGEConv(hidden_dim, hidden_dim)
        elif kind == "GAT":
            self.conv1 = GATConv(in_dim, hidden_dim, heads=2, concat=False, dropout=0.15)
            self.conv2 = GATConv(hidden_dim, hidden_dim, heads=2, concat=False, dropout=0.15)
        else:
            raise ValueError(f"Unknown backbone: {kind}")
        self.norm = nn.LayerNorm(hidden_dim)
        self.dropout = nn.Dropout(0.25)
        self.author_head = nn.Linear(hidden_dim, n_authors)
        self.misinfo_head = nn.Linear(hidden_dim, 2)

    def forward(self, x, edge_index):
        h = F.relu(self.norm(self.conv1(x, edge_index)))
        h = self.dropout(h)
        h = F.relu(self.norm(self.conv2(h, edge_index)))
        h = self.dropout(h)
        return self.author_head(h), self.misinfo_head(h)


def remove_author_links(edge_index, authors, user_to_node):
    """Remove both directed versions of each tweet's authorship edge."""
    n = len(authors)
    author_node = np.array([user_to_node[a] for a in authors], dtype=np.int64)
    source, target = edge_index.cpu().numpy()
    direct = np.zeros(len(source), dtype=bool)
    forward = (source < n) & (target >= n)
    reverse = (target < n) & (source >= n)
    direct[forward] = target[forward] == author_node[source[forward]]
    direct[reverse] = source[reverse] == author_node[target[reverse]]
    cleaned = edge_index[:, torch.from_numpy(~direct)]
    cs, ct = cleaned.cpu().numpy()
    f = (cs < n) & (ct >= n)
    r = (ct < n) & (cs >= n)
    residual = int((ct[f] == author_node[cs[f]]).sum() +
                   (cs[r] == author_node[ct[r]]).sum())
    assert residual == 0
    return cleaned, {"removed": int(direct.sum()), "remaining": int(cleaned.shape[1]),
                     "residual_authorship_edges": residual}


def run_experiment(X, edge_index, tweets, misinformation_labels, user_to_node,
                   epochs=35, hidden_dim=96, seed=42):
    """Compare GCN, GraphSAGE, and GAT on author and misinformation tasks.

    ``X`` must contain tweet nodes first. ``misinformation_labels`` contains
    the generated labels in the same row order as ``tweets``.
    """
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(seed)

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    n_tweets = len(tweets)
    X_t = torch.tensor(X, dtype=torch.float32, device=device).clone()
    X_t[:n_tweets, -2:] = 0  # author follower/following counts
    cleaned_edges, audit = remove_author_links(
        edge_index, tweets["author"].astype(str).to_numpy(), user_to_node)
    edge_t = cleaned_edges.to(device)
    names = sorted(tweets["author"].astype(str).unique())
    author_to_id = {name: i for i, name in enumerate(names)}
    y_author = torch.tensor(tweets["author"].astype(str).map(author_to_id).values,
                            dtype=torch.long, device=device)
    y_misinfo = torch.tensor((np.asarray(misinformation_labels) != "valid").astype(np.int64),
                             dtype=torch.long, device=device)

    ids = np.arange(n_tweets)
    train_ids, test_ids = train_test_split(ids, test_size=0.20, random_state=42,
                                           stratify=y_author.cpu().numpy())
    train_ids, val_ids = train_test_split(train_ids, test_size=0.20, random_state=42,
                                          stratify=y_author[train_ids].cpu().numpy())
    masks = []
    for chosen in (train_ids, val_ids, test_ids):
        mask = torch.zeros(n_tweets, dtype=torch.bool, device=device)
        mask[chosen] = True
        masks.append(mask)
    train_mask, val_mask, test_mask = masks

    def evaluate(model):
        model.eval()
        with torch.no_grad():
            author_logits, misinfo_logits = model(X_t, edge_t)
            author_pred = author_logits[:n_tweets][test_mask].argmax(1).cpu().numpy()
            misinfo_pred = misinfo_logits[:n_tweets][test_mask].argmax(1).cpu().numpy()
        author_true = y_author[test_mask].cpu().numpy()
        misinfo_true = y_misinfo[test_mask].cpu().numpy()
        return {
            "author_accuracy": accuracy_score(author_true, author_pred),
            "author_macro_f1": f1_score(author_true, author_pred, average="macro"),
            "misinfo_accuracy": accuracy_score(misinfo_true, misinfo_pred),
            "misinfo_macro_f1": f1_score(misinfo_true, misinfo_pred, average="macro"),
            "misinfo_balanced_accuracy": balanced_accuracy_score(misinfo_true, misinfo_pred),
            "misinfo_confusion_matrix": confusion_matrix(misinfo_true, misinfo_pred).tolist(),
        }

    results = {}
    for kind in ("GCN", "GraphSAGE", "GAT"):
        model = MultiTaskGNN(kind, X.shape[1], hidden_dim, len(names)).to(device)
        optimizer = torch.optim.AdamW(model.parameters(), lr=0.008, weight_decay=1e-4)
        best_state, best_score, patience = None, -1.0, 0
        started = time.time()
        for epoch in range(1, epochs + 1):
            model.train()
            optimizer.zero_grad()
            author_logits, misinfo_logits = model(X_t, edge_t)
            author_loss = F.cross_entropy(author_logits[:n_tweets][train_mask], y_author[train_mask])
            misinfo_loss = F.cross_entropy(
                misinfo_logits[:n_tweets][train_mask], y_misinfo[train_mask],
                weight=torch.tensor([1.0, 2.0], device=device))
            loss = author_loss + misinfo_loss
            loss.backward()
            optimizer.step()

            model.eval()
            with torch.no_grad():
                val_author_logits, val_misinfo_logits = model(X_t, edge_t)
                val_author = val_author_logits[:n_tweets][val_mask].argmax(1).cpu().numpy()
                val_misinfo = val_misinfo_logits[:n_tweets][val_mask].argmax(1).cpu().numpy()
            val_score = (f1_score(y_author[val_mask].cpu(), val_author, average="macro") +
                         f1_score(y_misinfo[val_mask].cpu(), val_misinfo, average="macro")) / 2
            if val_score > best_score:
                best_score = val_score
                best_state = copy.deepcopy(model.state_dict())
                patience = 0
            else:
                patience += 1
            if patience >= 8:
                break
        model.load_state_dict(best_state)
        result = evaluate(model)
        result["epochs_ran"] = epoch
        result["seconds"] = time.time() - started
        results[kind] = result
    text = tweets["text"].fillna("").astype(str)
    vectorizer = TfidfVectorizer(ngram_range=(1, 2), min_df=2,
                                 max_features=30000, sublinear_tf=True)
    z_train = vectorizer.fit_transform(text.iloc[train_ids])
    z_test = vectorizer.transform(text.iloc[test_ids])
    baseline = {}
    truth = y_misinfo[test_mask].cpu().numpy()
    for name, clf in (("majority", DummyClassifier(strategy="most_frequent")),
                      ("text_logreg", LogisticRegression(max_iter=1000,
                                     class_weight="balanced", random_state=42))):
        clf.fit(z_train, y_misinfo[train_mask].cpu().numpy())
        pred = clf.predict(z_test)
        baseline[name] = {
            "accuracy": accuracy_score(truth, pred),
            "macro_f1": f1_score(truth, pred, average="macro"),
            "balanced_accuracy": balanced_accuracy_score(truth, pred),
            "confusion_matrix": confusion_matrix(truth, pred).tolist(),
        }
    return {"audit": audit, "gnn": results, "baselines": baseline}


def run_three_seeds(X, edge_index, tweets, misinformation_labels, user_to_node):
    """Keep the tweet split fixed while varying model initialization."""
    runs = [run_experiment(X, edge_index, tweets, misinformation_labels,
                           user_to_node, seed=seed) for seed in (42, 43, 44)]
    return {"audit": runs[0]["audit"], "baselines": runs[0]["baselines"],
            "gnn": {kind: [run["gnn"][kind] for run in runs]
                    for kind in ("GCN", "GraphSAGE", "GAT")}}
