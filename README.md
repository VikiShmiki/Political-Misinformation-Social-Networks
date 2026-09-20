# Political Tweet Graph Learning: a Leakage-Audited Comparison

This course project compares GCN, GraphSAGE, and GAT on two tweet-level tasks: predicting one of 14 political accounts and predicting a **model-generated** misinformation label. The label is not a verified fact-check.

## Main finding

The first version of the graph directly connected every tweet to its author. That made author prediction partly an edge lookup. We removed all 14,106 directed authorship edges (zero remain) and zeroed the two author-profile-count features. The corrected graph has 7,053 tweet nodes, 3,107 account nodes, and 83,258 directed retweeter edges.

On the same fixed author-stratified split, the corrected models give:

| Model | Author macro-F1 | Pseudo-label macro-F1 |
|---|---:|---:|
| Majority class | — | 0.428 |
| Text-only logistic regression | — | 0.703 |
| GCN | 0.820 ± 0.007 | 0.688 ± 0.003 |
| GraphSAGE | 0.857 ± 0.016 | 0.702 ± 0.009 |
| GAT | 0.865 ± 0.003 | 0.669 ± 0.004 |

GNN values are mean ± sample standard deviation over model-initialization seeds 42, 43, and 44; the tweet split is held fixed. GraphSAGE is the best GNN for the pseudo-label task, but it is essentially tied with the text-only baseline. The earlier near-perfect author scores came from the leaky graph and should not be used as primary results.

## Data and method

The 7,053 tweets come from 14 Macedonian political accounts. A zero-shot multilingual model (`joeddav/xlm-roberta-large-xnli`) generated `valid`, `misleading`, and `invalid` labels. The binary task treats `misleading` and `invalid` as positive. Counts are 5,303 valid, 666 misleading, and 1,084 invalid. No human verification was done.

The models use two graph layers, a shared 96-dimensional representation, and separate author and pseudo-label heads. The split contains 4,513 train, 1,129 validation, and 1,411 test tweets. Model selection uses post-update validation macro-F1, averaged across tasks. The majority and class-balanced unigram/bigram TF-IDF logistic baselines use the same train/test tweets. The graph is transductive, and the GNN's 256-dimensional TF-IDF features were fitted on all *unlabeled* tweets during the original preparation; the text baseline's vectorizer is fitted on training tweets only. See the paper for implications.

## Reproduce in Colab

1. Open the [Colab notebook](https://colab.research.google.com/drive/1fUNYa80G0whRFzqyZnBW5XPjM6sxK9yH) and choose a GPU runtime.
2. Upload `political_tweets_dataset.zip` when prompted. The archive is not committed to this repository.
3. Run the preparation and pseudo-label cells. This recreates `/content/translated_labeled_tweets.csv` and may take several minutes.
4. Run the leakage-audit/text-baseline cell, corrected three-GNN cell, and three-seed stability cell near the end. Earlier cells in the notebook show exploratory, unaudited results and are retained for context, **not** the final result.

The reusable implementation is [`src/multitask_gnn.py`](src/multitask_gnn.py). It takes the original feature matrix and graph, the tweet DataFrame, generated labels, and a mapping from account name to graph node ID. `run_three_seeds(X, edge_index, df, labeled_df["misinformation_label"], user_to_node)` applies the audit and runs the baselines and corrected models with a fixed split. If the raw graph is rebuilt, preserve the tweet-first node ordering and the 256-text-plus-five-numeric-feature layout.

## Files

- [`report.pdf`](report.pdf): two-column paper with the corrected findings.
- [`report.tex`](report.tex): editable LaTeX source (`IEEEtran`, `booktabs`).
- [`src/multitask_gnn.py`](src/multitask_gnn.py): audited experiment and baselines.
- [`misinformation notebooks/`](misinformation%20notebooks/): original exploratory data notebooks.

The raw archive and derived labeled CSV are intentionally excluded from Git. The project does **not** claim to detect factual misinformation; it models automatically generated pseudo-labels.
