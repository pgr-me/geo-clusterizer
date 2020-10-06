#!/usr/bin/env python
# coding: utf-8

# standard library imports
import argparse
from pathlib import Path

# third party imports
from loguru import logger
import pickle
import numpy as np
import pandas as pd
import linearcorex as lc
from sklearn.mixture import GaussianMixture
from sklearn.model_selection import train_test_split

# local imports
from settings import (
    INTERIM_DIR,
    PROCESSED_DIR,
    MAX_COMPONENTS,
    MODELS_DIR,
    N_SAMPLES,
    RANDOM_STATE,
)


def find_elbow(s: pd.Series, keep="last") -> dict:
    """Find the number of components at the elbow of a BIC scree plot"""
    if type(s) is not pd.Series:
        raise TypeError("Must provide Pandas Series object as an argument")
    frame = s.rename("bic").to_frame()
    frame["bic_pct_change"] = frame["bic"].pct_change()
    frame["bic_pct_change2"] = frame["bic_pct_change"].pct_change()
    frame["criteria_1"] = frame["bic_pct_change2"] < 0
    frame["criteria_2"] = frame["bic_pct_change2"].shift(1) > 0
    frame["criteria_3"] = frame["bic_pct_change"].shift(-1) < frame["bic_pct_change"]
    frame["criteria_4"] = frame.isnull().sum(axis=1) == 0
    frame["criteria_5"] = frame.shift(-1)["bic_pct_change"] < 0
    frame["score"] = frame.loc[:, "criteria_1":"criteria_5"].sum(axis=1)
    return {
        "elbow": frame.drop_duplicates("score", keep=keep)["score"].idxmax(),
        "scores": frame,
    }


def label_data(frame, model):
    """Predict cluster label for each tract"""
    frame["cluster"] = model.predict(X)
    ix = ["geoid", "state_abbr", "logrecno", "geo_label", "cluster"]
    return frame.reset_index().set_index(ix)


def train_gaussian_mixture_models(
    X: np.array, n_components_li, random_state, verbose=False
):
    """Train a set of Gaussian Mixture models and summary statistics for each model"""
    gm_outputs = {}
    for n_components in n_components_li:
        gm = GaussianMixture(
            n_components=n_components,
            n_init=1,
            covariance_type="full",
            warm_start=True,
            verbose=verbose,
            random_state=random_state,
        )
        gm.fit(X)
        aic = gm.aic(X)
        bic = gm.bic(X)
        if verbose:
            print(f"n_components={n_components}, AIC={round(aic)}, BIC={round(bic)})")
        gm_outputs[n_components] = {"model": gm, "aic": aic, "bic": bic}
    return gm_outputs


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
        description = "Train Gaussian Mixture Model and cluster tracts"
        parser = argparse.ArgumentParser(description=description)
        parser.add_argument(
            "-c",
            "--max_components",
            default=MAX_COMPONENTS,
            help="Maximum number of components",
            type=int,
        )
        parser.add_argument(
            "-i",
            "--interim_dir",
            default=INTERIM_DIR,
            help="Path to interim data directory",
            type=Path,
        )
        parser.add_argument(
            "-m",
            "--models_dir",
            default=MODELS_DIR,
            help="Path to models directory",
            type=Path,
        )
        parser.add_argument(
            "-n",
            "--n_samples",
            default=N_SAMPLES,
            help="Number of samples to draw for Corex training set",
            type=int,
        )
        parser.add_argument(
            "-p",
            "--processed_dir",
            default=PROCESSED_DIR,
            help="Path to processed data directory",
            type=Path,
        )
        parser.add_argument(
            "-r",
            "--random_state",
            default=RANDOM_STATE,
            help="Path to processed data directory",
            type=Path,
        )
        args = parser.parse_args()
        max_components = args.max_components
        ce_src = args.processed_dir / "selected_n_components.pkl"
        src = args.processed_dir / "scaled_imputed_data.pkl"
        orig_src = args.interim_dir / "acs__preprocessed_tables.pkl"
        gm_dst = args.models_dir / "gaussian_mixture.pkl"
        ce_dst = args.models_dir / "corex.pkl"
        ce_map_dst = args.models_dir / "ce_map.pkl"
        labeled_dst = args.processed_dir / "labeled.pkl"
        labeled_orig_dst = args.processed_dir / "labeled_orig.pkl"
        random_state = args.random_state
        logger.debug("Finish parsing arguments")
    except Exception:
        logger.error("Failed to parse arguments", exc_info=True)
        raise

    # @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
    print("Load data")
    try:
        df_orig = pd.read_pickle(orig_src)
        df = pd.read_pickle(src)
        with open(str(ce_src), "rb") as f:
            ce_obj = pickle.load(f)
        selected_n_components = ce_obj["n_components"]
        logger.debug("Finished loading data")
    except Exception:
        logger.error("Failed to load data", exc_info=True)
        raise

    # @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
    print("Train Corex model using selected number of components")
    try:
        ce_model = lc.Corex(
            n_hidden=selected_n_components,
            gaussianize="outliers",
            verbose=True,
            seed=RANDOM_STATE,
        )
        ce_model.fit(
            df.sample(N_SAMPLES, random_state=RANDOM_STATE, replace=True).values
        )
    except Exception:
        logger.error("Failed to train Corex model", exc_info=True)
        raise

    # @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
    print("Map hidden layers to original features")
    try:
        index = [x + 1 for x in range(selected_n_components)]
        ce_map = (
            pd.Series(ce_model.clusters(), index=df.columns, name="component")
            .to_frame()
            .sort_index()
            .sort_values(by="component")
        )
    except Exception:
        logger.error("Failed to map hidden layers to original features", exc_info=True)
        raise

    # @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
    print("Select optimal number of clusters and train best model")
    try:
        X = pd.DataFrame(ce_model.transform(df), index=df.index)
        outputs = train_gaussian_mixture_models(
            X, list(range(2, max_components)), random_state
        )
        bic = pd.DataFrame.from_dict(outputs, orient="index").bic
        elbow_di = find_elbow(bic)
        elbow = elbow_di["elbow"]
        selected_gm_model = outputs[elbow]["model"]
        logger.debug("Selected optimal number of clusters and trained best model")
    except Exception:
        logger.error(
            "Failed to select optimal number of clusters / train best model",
            exc_info=True,
        )
        raise

    # @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
    print(f"Save outputs")
    try:
        # corex and gaussian mixture models
        with open(str(ce_dst), "wb") as f:
            pickle.dump(ce_model, f)
        with open(str(gm_dst), "wb") as f:
            pickle.dump(selected_gm_model, f)
        # labeled, scaled data
        csv_dst = labeled_dst.parents[0] / f"{labeled_dst.stem}.csv"
        labeled_data = label_data(df, selected_gm_model)
        labeled_data.to_pickle(labeled_dst)
        labeled_data.to_csv(csv_dst)
        # labeled, unscaled data
        csv_dst = labeled_orig_dst.parents[0] / f"{labeled_orig_dst.stem}.csv"
        labeled_orig_data = label_data(df_orig, selected_gm_model)
        labeled_orig_data.to_pickle(labeled_orig_dst)
        labeled_orig_data.to_csv(csv_dst)
        # corex map of features to hidden layers
        ce_map.to_csv(ce_map_dst)
        logger.debug(f"Finished saving outputs")
    except Exception:
        logger.error("Failed to save outputs", exc_info=True)
        raise
