"""Small checks for the evaluation invariants that affect scientific conclusions."""

import sys
import unittest
from pathlib import Path

import numpy as np
import pandas as pd
import torch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from extended_experiments import (ModelConfig, ResidualNetwork, build_graph,
                                  choose_threshold, induce_subgraph, make_split,
                                  prepare_features)
from prepare_labels import confidence_gate, materialize_labels


class PipelineChecks(unittest.TestCase):
    def test_missing_text_is_unlabeled_not_valid(self):
        df = pd.DataFrame({"eligible": [True, False]})
        scores = np.array([[[.9,.06,.04],[.9,.06,.04]], [[1/3]*3,[1/3]*3]])
        labeled = materialize_labels(df, scores)
        self.assertEqual(labeled.target.iloc[1], -1)
        self.assertEqual(labeled.misinformation_label.iloc[1], "unlabeled")
        self.assertFalse(labeled.accepted.iloc[1])
        self.assertEqual(labeled.confidence.iloc[1], 0)

    def test_confidence_and_disagreement_abstain(self):
        scores = np.array([
            [[.90, .06, .04], [.85, .10, .05]],
            [[.05, .85, .10], [.08, .12, .80]],
            [[.85, .10, .05], [.10, .85, .05]],
            [[.34, .33, .33], [.35, .33, .32]],
            [[.90, .06, .04], [.90, .06, .04]],
        ])
        accepted, agreement, _, _ = confidence_gate(scores, [True, True, True, True, False])
        np.testing.assert_array_equal(accepted, [True, True, False, False, False])
        self.assertFalse(agreement[2])

    def test_author_edges_removed_and_neighbors_remapped(self):
        df = pd.DataFrame({"author": ["a", "b", "a"],
                           "retweeters": ["a, shared, shared", "shared", "None"]})
        edges, n_nodes, audit = build_graph(df)
        self.assertEqual(audit["residual_authorship_edges"], 0)
        self.assertEqual(audit["directed_edges"], 4)
        self.assertEqual(audit["tweet_nodes_without_retweeter_edges"], 1)
        sx, se, nodes = induce_subgraph(torch.zeros(n_nodes, 2), edges, np.array([1, 2]), 3)
        self.assertEqual(len(sx), 3)
        self.assertEqual(se.shape[1], 2)
        self.assertLess(int(se.max()), len(sx))
        np.testing.assert_array_equal(nodes[:2], [1, 2])

    def test_duplicate_text_never_crosses_partitions(self):
        n = 300
        df = pd.DataFrame({"author": [f"a{i % 3}" for i in range(n)],
                           "target": [i % 2 for i in range(n)],
                           "clean_text": [f"group {i // 2}" for i in range(n)],
                           "eligible": True})
        split = make_split(df)
        groups = [set(df.clean_text.iloc[v]) for v in split.values()]
        for i in range(3):
            for j in range(i):
                self.assertFalse(groups[i] & groups[j])
        self.assertEqual(sum(map(len, split.values())), n)

    def test_test_only_token_does_not_enter_vocabulary(self):
        df = pd.DataFrame({"clean_text": ["training common", "training common",
                                         "testonlyword", "testonlyword"],
                           "tweet.retweet_count": [0, 2, 1000, 1000],
                           "tweet.reply_count": [0, 0, 0, 0],
                           "tweet.favorite_count": [0, 0, 0, 0]})
        base, rich = prepare_features(df, {"train": np.array([0, 1])}, 5, np.zeros((4, 3)))
        self.assertEqual(int(torch.count_nonzero(base[2, :-3])), 0)
        self.assertEqual(int(torch.count_nonzero(base[4])), 0)
        self.assertGreater(float(base[2, -3]), 5)
        self.assertEqual(rich.shape[1], base.shape[1] + 3)

    def test_checkpoint_threshold_uses_validation_tradeoff(self):
        threshold, score = choose_threshold(np.array([0, 0, 1, 1]), np.array([.1, .2, .35, .4]))
        self.assertLess(threshold, .4)
        self.assertEqual(score, 1.0)

    def test_large_model_backpropagates_and_mlp_ignores_edges(self):
        torch.set_num_threads(1)
        x = torch.randn(8, 7)
        edge = torch.tensor([[0, 1, 2, 3], [1, 0, 3, 2]])
        cfg = ModelConfig("test", hidden=12, layers=4)
        model = ResidualNetwork(7, 3, cfg)
        a, b = model(x, edge)
        self.assertEqual(tuple(a.shape), (8, 3))
        self.assertEqual(tuple(b.shape), (8, 2))
        (a[:4].square().mean() + b[:4].square().mean()).backward()
        self.assertTrue(all(p.grad is not None for p in model.parameters()))
        mlp = ResidualNetwork(7, 3, ModelConfig("mlp", kind="MLP", hidden=12)).eval()
        first = mlp(x, edge)[1]
        second = mlp(x, torch.empty((2, 0), dtype=torch.long))[1]
        self.assertTrue(torch.equal(first, second))


if __name__ == "__main__":
    unittest.main()
