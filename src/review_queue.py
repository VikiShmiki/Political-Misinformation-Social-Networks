"""Create a local, stratified queue for independent annotation (never fill gold labels)."""

import argparse
from pathlib import Path

import pandas as pd


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--labels", required=True)
    parser.add_argument("--out", default="work/manual_review.csv")
    parser.add_argument("--per-stratum", type=int, default=20)
    args = parser.parse_args()
    df = pd.read_csv(args.labels, dtype={"tweet_id": str})
    selected = []
    for accepted in (False, True):
        for target in (0, 1):
            group = df[(df.eligible) & (df.accepted == accepted) & (df.target == target)]
            selected.append(group.sample(n=min(args.per_stratum, len(group)), random_state=42))
    short = df[~df.eligible]
    selected.append(short.sample(n=min(args.per_stratum, len(short)), random_state=42))
    queue = pd.concat(selected).drop_duplicates("tweet_id")
    columns = ["tweet_id", "author", "text", "eligible", "misinformation_label",
               "descriptive_label", "target", "accepted", "confidence", "margin"]
    teacher_metadata = queue[columns].copy()
    queue = queue[["tweet_id", "author", "text"]].copy()
    queue["human_label"] = ""
    queue["evidence_url"] = ""
    queue["annotation_rationale"] = ""
    queue["annotator"] = ""
    out = Path(args.out)
    metadata_path = out.with_name(out.stem + "-teacher-metadata.csv")
    if out.exists() or metadata_path.exists():
        raise FileExistsError(f"Preserve existing annotations: {out}")
    out.parent.mkdir(parents=True, exist_ok=True)
    queue.to_csv(out, index=False)
    teacher_metadata.to_csv(metadata_path, index=False)
    print(f"Created {len(queue)} blinded review rows; all human annotation fields are blank.")
    print("Keep the separate teacher metadata file hidden until annotations are complete.")


if __name__ == "__main__":
    main()
