# Political Misinformation in a Social Network

This project builds a graph from political tweets and compares three graph neural network (GNN) architectures on two related tasks:

1. predict which political account authored a tweet;
2. predict whether the tweet received a generated misinformation label.

The graph contains 7,053 tweets, 10,160 total nodes, and 97,364 edges. The experiment compares a GCN, GraphSAGE, and GAT with a shared encoder and two prediction heads.

## Results

All models use the same fixed stratified train/validation/test split.

| Model | Author accuracy | Author macro-F1 | Misinformation accuracy | Misinformation macro-F1 |
|---|---:|---:|---:|---:|
| GCN | 0.962 | 0.957 | 0.755 | 0.689 |
| GraphSAGE | 0.991 | 0.989 | 0.775 | 0.708 |
| GAT | 0.997 | 0.997 | 0.703 | 0.641 |

GraphSAGE is the strongest balanced model. GAT is excellent at identifying the account but less reliable for the misinformation task.

Additional experiments were also run. In single-task controls, the author/misinformation macro-F1 pairs were 0.952/0.683 for GCN, 0.998/0.693 for GraphSAGE, and 0.994/0.700 for GAT. GraphSAGE training-size sensitivity produced misinformation macro-F1 values of 0.689, 0.712, and 0.708 with 50%, 75%, and 100% of the training tweets, respectively. These checks are discussed in the paper.

## Important label note

The misinformation labels are pseudo-labels produced with the multilingual zero-shot model `joeddav/xlm-roberta-large-xnli`, using the candidate labels `valid`, `misleading`, and `invalid`. They are not manually verified fact-checks. Therefore, the second task should be described as prediction of automatically generated misinformation labels, not definitive truth detection.

## Running in Google Colab

1. Open the [Colab notebook](https://colab.research.google.com/drive/1fUNYa80G0whRFzqyZnBW5XPjM6sxK9yH).
2. Select a GPU runtime.
3. Upload the raw `political_tweets_dataset.zip` archive when prompted.
4. Run the preprocessing and labeling cells.
5. Run the multi-task comparison cell.

The reusable model and training function are in [`src/multitask_gnn.py`](src/multitask_gnn.py). The original cleanup and graph-building notebooks remain in [`misinformation notebooks/`](misinformation%20notebooks/).

The generated labeled CSV and raw archive are intentionally excluded from Git because they are large derived data files. The preprocessing notebook recreates the labeled file in the Colab runtime.

## Project structure

```text
src/multitask_gnn.py              model and experiment code
misinformation notebooks/        original data and graph notebooks
report.tex                       two-column paper source
report.pdf                       rendered two-column paper
requirements.txt                 Python dependencies
```
