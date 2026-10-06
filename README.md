# Political Tweet Graph Learning

A course project on author identification and **model-generated valid/misleading/invalid labels** for 7,053 tweets from 14 Macedonian political accounts. These labels are not verified fact-checks.

The October revision studies the whole pipeline: confidence-aware labeling, train-only preprocessing, multilingual text features, larger residual graph networks, controlled ablations, and measured runtime scaling. The updated paper uses the IEEE two-column conference format.

- [Paper](report.pdf) and [LaTeX source](report.tex)
- [Measured results](results/revision-v2/RESULTS.md), [individual runs](results/revision-v2/runs), and [runtime samples](results/revision-v2/scaling.json)
- [Open the revised notebook in Colab](https://colab.research.google.com/github/VikiShmiki/Political-Misinformation-Social-Networks/blob/main/notebooks/revised_pipeline.ipynb)

## What changed

The original author graph contained direct tweet-to-author links. Those exposed the target through the edges. The corrected graph removes them, excludes own-author retweeter links, deduplicates edges, and omits author follower/following features. Every reconstruction asserts that zero direct authorship edges remain.

The new label stage uses a pinned multilingual mDeBERTa NLI teacher in float32. It retains three candidate scores from each of two prompt formulations. A label is accepted for filtered training only when the text has at least five words after cleaning, both prompts agree on the binary class, both binary confidence scores are at least 0.75, and both three-way margins are at least 0.15. Other assignments are marked uncertain. A known entailment/contradiction/neutral sanity check runs before generation. Generation resumes from saved score chunks.

The first prompt always defines the target: the most likely three-way class is mapped to valid = 0 and misleading/invalid = 1. Confidence filtering changes supervision, **not the test population**. All models are evaluated on the same eligible test tweets, including teacher-abstained rows. Agreement between prompts is a consistency check; it does not establish factual correctness.

The split keeps equal case-folded, cleaned tweet text together. TF-IDF and activity scaling are fitted on training tweets only. The graph remains transductive: permitted test-node features/edges are visible, but held-out labels never enter the loss. This is not a chronological or new-account test.

## Comparisons

Ten neural configurations use initialization seeds 42, 43, and 44 on one fixed grouped split:

| Configuration | What it tests |
|---|---|
| SAGE-TFIDF | 256 lexical dimensions + three activity features |
| GCN-small / SAGE-small / GAT-small | Two residual blocks, width 96; add 384 frozen multilingual dimensions |
| SAGE-filtered | Same small GraphSAGE with accepted/weighted binary supervision |
| SAGE-wide | Two blocks, width 192, filtered supervision |
| SAGE-deep | Four blocks, width 192, filtered supervision |
| MLP-control | Same features, depth, width, and supervision as deep GraphSAGE; no graph |
| SAGE-no-edges | Exact deep GraphSAGE parameterization with every edge removed |
| SAGE-single-task | Deep GraphSAGE with author loss disabled |

Unfiltered models use all eligible training pseudo-labels. Filtered models use accepted binary labels, while the author head still learns from all eligible training screen names. AdamW runs for at most 70 epochs with patience 12. Validation binary macro-F1 selects checkpoints and a threshold from 0.10 to 0.90 in 0.05 steps. Test labels never select a threshold or checkpoint.

The controls include majority prediction and class-balanced word unigram/bigram logistic regression, with all-label and filtered fits. Results retain precision/recall, average precision, ROC-AUC, Brier scores, confusion matrices, and default-threshold metrics. Paired bootstrap comparisons resample duplicate-text groups and are conditional on this teacher and split.

## Recorded findings

The recorded study's clearest gain is the feature change: small GraphSAGE improves from 0.743 to 0.812 binary macro-F1 with multilingual sentence features. Small GCN obtains 0.818. Deep filtered GraphSAGE reaches 0.800, approximately tied with filtered small GraphSAGE, at 4.08 times its all-label small counterpart's warm training-step cost. All three reported paired larger-model comparisons have intervals containing zero. Confidence filtering makes uncertain supervision explicit, but does not improve aggregate F1 here; these remain teacher-agreement scores.

## Runtime experiment

Small/wide/deep GraphSAGE and small GAT are benchmarked at 12.5%, 25%, 50%, and 100% of real tweet nodes, with their account neighbors. Samples are nested; no synthetic or repeated tweets inflate the collection.

The benchmark records a first optimizer step, discards two warm-up steps, and retains seven warm training-step and inference timings. A training step includes forward/backward propagation and AdamW. It excludes teacher generation, embedding extraction, validation, and I/O. All configurations use the same eligible loss rows in this benchmark to isolate architecture cost. The full graph includes isolated account nodes as well. CPU execution completes before timing ends, and CUDA runs synchronize explicitly. Median/IQR and actual node/edge counts are saved.

Published measurements describe one Intel Core i7-1255U laptop with four PyTorch CPU threads. They are not GPU benchmarks or predictions of million-node performance. Teacher throughput is measured separately at three NLI pair batch sizes, after the main jobs have finished.

## Reproduce locally

Use Python 3.11 or 3.12 and an isolated environment. For the exact recorded CPU package versions:

```bash
python -m pip install -r requirements-lock.txt --extra-index-url https://download.pytorch.org/whl/cpu
python -m unittest discover -s tests -v
```

For Colab/GPU or another supported environment, install `requirements.txt`; the scripts record actual package versions and backend in the manifest. The frozen model revisions remain pinned. Keep the original `political_tweets_dataset.zip` locally; it is not included in Git.

```bash
python src/prepare_labels.py --archive path/to/political_tweets_dataset.zip --out work/labels-v2-fp32

python src/extended_experiments.py --labels work/labels-v2-fp32/labeled_tweets.csv --out results/local-run

python src/benchmark_teacher.py --archive path/to/political_tweets_dataset.zip --out results/local-run/teacher_timing.json

python src/build_artifacts.py --results results/local-run --predictions work/experiment-cache/local-run/predictions --labels work/labels-v2-fp32/labeled_tweets.csv --label-manifest work/labels-v2-fp32/label_manifest.json

python src/review_queue.py --labels work/labels-v2-fp32/labeled_tweets.csv
```

Existing label caches resume only with an identical configuration. Existing model run files are reused only after manifest/config checks. Use a fresh output directory for a changed split, epoch limit, model definition, or label file; do not mix runs from different settings. The teacher benchmark and review exporter refuse to overwrite existing outputs.

`build_artifacts.py` requires all ten configurations and all three seeds for the paper. It regenerates the numerical macros, tables, plots, label audit, and paired comparisons. Figures are editable TikZ/PGFPlots sources.

The published paper reads the recorded `results/revision-v2/latex` artifacts. The commands above write fresh results and LaTeX artifacts under `results/local-run`; point the paper's input paths at that directory to typeset a rerun without changing the published measurements.

Compile the multi-file paper with a LaTeX installation providing IEEEtran, booktabs, TikZ, and PGFPlots, or:

```bash
tectonic report.tex --outdir work/latex-build
```

For a quick implementation smoke check, use the unit tests rather than treating a tiny synthetic run as a scientific result. Tests cover confidence abstention, duplicate grouping, graph leakage, subgraph remapping, train-only vocabulary/scaling, large-model gradients, and a training/checkpoint/export cycle.

## Human annotation

`work/manual_review.csv` is a local blinded sample across accepted/rejected binary strata plus short texts. Its `human_label`, `evidence_url`, `annotation_rationale`, and `annotator` fields are blank. Keep the separate teacher-metadata CSV hidden until annotation is finished. Reviewers should distinguish factual claims from opinions and insufficient evidence, and consult independent sources before assigning truth-related labels. No automatically filled field is counted as human verification.

The repository excludes raw texts, generated labeled CSVs, teacher scores, trained weights, and review files. Only aggregate results, row-index split manifests, code, and report artifacts are published.

## Relation to the September results

The previous leakage-audited experiment reported author macro-F1 of 0.820/0.857/0.865 for GCN/GraphSAGE/GAT and generated-label F1 of 0.688/0.702/0.669. Text logistic regression reached 0.703. Those results used XLM-R-generated labels and the earlier split/features. They are retained in Git history, not combined with the new results. The teacher, preparation, and split changed, so differences across revisions cannot be attributed to a bigger model. The controlled comparisons above provide that evidence.

`src/multitask_gnn.py` remains the reusable legacy implementation. The original exploratory notebooks are preserved under `misinformation notebooks/`; their unaudited near-perfect author scores must not be used as final evidence. The revised notebook and paper are the current entry points.

## Sources

The project uses [GCN](https://arxiv.org/abs/1609.02907), [GraphSAGE](https://arxiv.org/abs/1706.02216), and [GAT](https://arxiv.org/abs/1710.10903). Frozen text features use [multilingual MiniLM](https://huggingface.co/sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2); pseudo-labels use [multilingual mDeBERTa NLI](https://huggingface.co/MoritzLaurer/mDeBERTa-v3-base-mnli-xnli). Both model cards describe the intended use and limitations. This project measures teacher-label agreement, not verified misinformation detection.
