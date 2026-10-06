"""Measure label-generation throughput without model loading or download time."""

import argparse
import json
import os
import time
from pathlib import Path

os.environ.setdefault("USE_TF", "0")

import numpy as np
import torch
from transformers import AutoModelForSequenceClassification, AutoTokenizer

from prepare_labels import MODEL_ID, MODEL_REVISION, VIEWS, read_archive, score_pairs


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--tweets", type=int, default=24)
    parser.add_argument("--repeats", type=int, default=3)
    args = parser.parse_args()
    path = Path(args.out)
    if path.exists():
        raise FileExistsError(f"Refusing to overwrite {path}")
    torch.set_num_threads(args.threads)
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    df = read_archive(args.archive)
    ids = np.random.default_rng(42).choice(np.flatnonzero(df.eligible), args.tweets, replace=False)
    texts = df.clean_text.iloc[ids].tolist()
    tokenizer = AutoTokenizer.from_pretrained(MODEL_ID, revision=MODEL_REVISION)
    model = AutoModelForSequenceClassification.from_pretrained(MODEL_ID, revision=MODEL_REVISION)
    model.float().eval().to(device)
    rows = []
    for batch_size in (3, 12, 24):
        score_pairs(model, tokenizer, texts[:4], VIEWS[0], batch_size, 128, device)
        durations = []
        for _ in range(args.repeats):
            if device.type == "cuda":
                torch.cuda.synchronize(device)
            start = time.perf_counter()
            score_pairs(model, tokenizer, texts, VIEWS[0], batch_size, 128, device)
            if device.type == "cuda":
                torch.cuda.synchronize(device)
            durations.append(time.perf_counter() - start)
        row = {"pair_batch_size": batch_size, "tweets": len(texts),
               "seconds": durations, "median_seconds": float(np.median(durations)),
               "tweets_per_second": float(len(texts) / np.median(durations))}
        rows.append(row)
        print(json.dumps(row), flush=True)
    result = {"model": MODEL_ID, "revision": MODEL_REVISION, "dtype": "float32",
              "device": str(device), "threads": args.threads, "max_length": 128,
              "sample_seed": 42, "sample_rows": ids.tolist(), "rows": rows,
              "note": "One prompt view; three NLI pairs/tweet; includes tokenization; excludes loading."}
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(result, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
