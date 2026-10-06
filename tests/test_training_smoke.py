"""Verify a complete training/checkpoint/export cycle on explicitly synthetic data."""

import sys
import tempfile
import unittest
from pathlib import Path

import numpy as np
import pandas as pd
import torch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from extended_experiments import ModelConfig, fit_model


class TrainingSmoke(unittest.TestCase):
    def test_filtered_training_checkpoint_and_exports(self):
        torch.set_num_threads(1)
        n = 60
        x = torch.randn(n, 9)
        edge = torch.tensor([[i for i in range(n - 1)], [i + 1 for i in range(n - 1)]])
        author = torch.arange(n) % 3
        label = torch.arange(n) % 2
        split = {"train": np.arange(0, 40), "val": np.arange(40, 50), "test": np.arange(50, 60)}
        df = pd.DataFrame({"accepted": [i % 3 != 0 for i in range(n)], "confidence": .9})
        cfg = ModelConfig("synthetic-smoke", hidden=12, layers=4, filtered=True)
        with tempfile.TemporaryDirectory() as directory:
            result = fit_model(cfg, x, edge, author, label, split, df,
                               seed=42, epochs=2, device=torch.device("cpu"),
                               private_dir=directory, output_dir=directory)
            self.assertEqual(result["epochs_ran"], 2)
            self.assertLess(result["training_labels"], 40)
            self.assertEqual(result["label"]["n"], 10)
            self.assertEqual(sum(map(sum, result["label"]["confusion_matrix"])), 10)
            self.assertTrue(Path(directory, "synthetic-smoke-42.pt").exists())
            self.assertTrue(Path(directory, "runs", "synthetic-smoke-42.json").exists())


if __name__ == "__main__":
    unittest.main()
