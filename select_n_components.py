# standard library imports
import argparse
from copy import deepcopy
import json
import os
from pathlib import Path
import pickle

# third-party imports
from loguru import logger
import pandas as pd
import linearcorex as lc
from sklearn.decomposition import PCA

# local imports
from settings import (
    CE_CUTOFF,
    N_HIDDEN,
    N_SAMPLES,
    N_TRIALS,
    PROCESSED_DIR,
)
from src.acs import ACS


def make_corex_components_summary(
    frame: pd.DataFrame,
    n_runs: int,
    n_samples: int,
    n_hidden: int,
    ce_cutoff: float,
) -> pd.DataFrame:
    """Train multiple Linear Corex models using bootstrapped datasets to determine optimal number of clusters"""
    components_summary = {}
    for random_state in range(n_runs):
        frame = frame.sample(n_samples, random_state=random_state, replace=True)
        corex_model = lc.Corex(
            n_hidden=n_hidden, gaussianize="outliers", verbose=True, seed=random_state
        )
        corex_model.fit(frame.values)
        s = pd.Series(corex_model.tcs)
        corex_tc = s.rename("tc").to_frame()
        corex_tc["n_components"] = [x + 1 for x in range(len(corex_tc))]
        corex_tc.set_index("n_components", inplace=True)
        corex_tc.loc[0] = 0
        corex_tc.sort_index(inplace=True)
        corex_tc["cum_tc"] = corex_tc.cumsum()
        corex_tc["ce_cutoff"] = corex_tc['cum_tc'].pct_change()
        m = corex_tc["ce_cutoff"] > ce_cutoff
        components_summary[random_state] = {
            "n_components": corex_tc[m].index[-1],
            "n_samples": len(frame),
            "corex_tc": corex_tc
        }
    return components_summary


def select_n_components(components_summary: dict) -> int:
    """Select number of Linear Corex components"""
    n_components_li = [di['n_components'] for random_state, di in components_summary.items()]
    n_components = int(sum(n_components_li) / len(n_components_li))
    return n_components


if __name__ == "__main__":
    # @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
    print("Configure and instantiate logger")
    logger.add(
        f"log_{__file__}.log".replace(".py", ""), backtrace=False, diagnose=False
    )
    logger.debug(f"Begin {__file__}")

    # @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
    print("Parse arguments")
    try:
        default_input_src = PROCESSED_DIR / "scaled_imputed_data.pkl"
        default_output_dst = PROCESSED_DIR / "selected_n_components.pkl"
        description = "Select number of Corex components"
        parser = argparse.ArgumentParser(description=description)
        parser.add_argument(
            "-c",
            "--ce_cutoff",
            default=CE_CUTOFF,
            help="Cutoff used to select number of Corex components",
            type=int,
        )
        parser.add_argument(
            "-d",
            "--n_hidden",
            default=N_HIDDEN,
            help="Maximum number of Corex components",
            type=int,
        )
        parser.add_argument(
            "-i",
            "--input_src",
            default=default_input_src,
            help="Directory to save parsed ACS files",
            type=Path,
        )
        parser.add_argument(
            "-n",
            "--n_samples",
            default=N_SAMPLES,
            help="Number of samples to draw for each trial",
            type=int,
        )
        parser.add_argument(
            "-o",
            "--output_dst",
            default=default_output_dst,
            help="Path to pickled object that provides summary stats and selected number of Corex components",
            type=Path,
        )
        parser.add_argument(
            "-t",
            "--n_trials",
            default=N_TRIALS,
            help="Number of model run training trials",
            type=int,
        )
        args = parser.parse_args()
        logger.debug("Finish parsing arguments")
    except Exception:
        logger.error("Failed to parse arguments", exc_info=True)
        raise

    # @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
    print("Find optimal number of Corex components")
    try:
        df = pd.read_pickle(args.input_src)
        components_summary = make_corex_components_summary(
            df, args.n_trials, args.n_samples, args.n_hidden, args.ce_cutoff
        )
        n_components = select_n_components(components_summary)
        logger.debug(f"Found optimal number of Corex components: {n_components}")
    except Exception:
        logger.error("Failed to find optimal number of Corex components", exc_info=True)
        raise

    # @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
    print(f"Save outputs to {args.output_dst}")
    try:
        di = {
            "ce_cutoff": args.ce_cutoff,
            "n_components": n_components,
            "n_hidden": args.n_hidden,
            "n_samples": args.n_samples,
            "n_trials": args.n_trials,
            "ce_tc_df": components_summary
        }
        with open(str(args.output_dst), 'wb') as f:
            pickle.dump(di, f)
        logger.debug(f"Finished saving outputs to {args.output_dst}")
    except Exception:
        logger.error("Failed to save outputs", exc_info=True)
        raise
