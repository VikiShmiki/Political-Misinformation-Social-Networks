"""Print descriptive checks used in the paper for the raw tweet archive.

Usage: python src/data_audit.py path/to/political_tweets_dataset.zip
This script reads the raw CSV files directly from the ZIP; it does not use
or regenerate the misinformation pseudo-labels.
"""

import argparse
import re
from zipfile import ZipFile

import pandas as pd


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("archive", help="Path to political_tweets_dataset.zip")
    args = parser.parse_args()

    with ZipFile(args.archive) as zf:
        names = sorted(name for name in zf.namelist() if name.endswith(".csv"))
        frames = [pd.read_csv(zf.open(name), on_bad_lines="skip") for name in names]
    tweets = pd.concat(frames, ignore_index=True).drop_duplicates("tweet_id")
    text = tweets["tweet.full_text"].fillna("").astype(str).str.strip()
    url_only = text.str.match(re.compile(r"^(?:https?://\S+\s*)+$", re.I))
    retweeters = tweets["retweeters"].fillna("").astype(str).str.strip()
    has_retweeter = retweeters.map(
        lambda value: any(part.strip() and part.strip().lower() != "none"
                          for part in value.split(","))
    )

    print("CSV files:", len(names))
    print("Tweets:", len(tweets))
    print("Authors:", tweets["tweet.user.screen_name"].nunique())
    print("URL-only tweet texts:", int(url_only.sum()))
    print("Tweets without listed retweeters:", int((~has_retweeter).sum()))


if __name__ == "__main__":
    main()
