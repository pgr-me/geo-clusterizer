# standard library imports
import argparse
import os
from pathlib import Path
import pickle

# third-party imports
from loguru import logger
import pandas as pd
from sklearn.impute import SimpleImputer, MissingIndicator
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import QuantileTransformer, StandardScaler

# local imports
from settings import INTERIM_DIR, PROCESSED_DIR, RANDOM_STATE, MODELS_DIR


if __name__ == "__main__":
    """Scale and impute parsed, preprocessed ACS data.
    Last data processing step prior to modeling
    """
    # @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
    print("Configure and instantiate logger")
    logger.add(
        f"log_{__file__}.log".replace(".py", ""), backtrace=False, diagnose=False
    )
    logger.debug(f"Begin {__file__}")

    # @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
    print("Parse arguments")
    try:
        default_src = INTERIM_DIR / "acs__preprocessed_tables.pkl"
        default_dst = PROCESSED_DIR / "scaled_imputed_data.pkl"
        default_model_dst = MODELS_DIR / 'scaler_imputer.pkl'
        description = "Scale and impute parsed, preprocessed ACS data"
        parser = argparse.ArgumentParser(description=description)
        parser.add_argument(
            "-i",
            "--input_src",
            default=default_src,
            help="Path to parsed, preprocessed ACS data",
            type=Path,
        )
        parser.add_argument(
            "-m",
            "--model_dst",
            default=default_model_dst,
            help="Path to trained scale-impute model",
            type=Path,
        )
        parser.add_argument(
            "-o",
            "--output_dst",
            default=default_dst,
            help="Path to scaled, imputed data",
            type=Path,
        )
        parser.add_argument(
            "-r",
            "--random_state",
            default=RANDOM_STATE,
            help="Directory to save parsed ACS files",
            type=int,
        )
        args = parser.parse_args()
        input_src = args.input_src
        model_dst = args.model_dst
        models_dir = model_dst.parents[0]
        cache_dir = models_dir / "cache"
        cache_dir.mkdir(exist_ok=True)
        output_dst = args.output_dst
        random_state = args.random_state
        logger.debug("Finish parsing arguments")
    except Exception:
        logger.error("Failed to parse arguments", exc_info=True)
        raise

    # @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
    print("Scale and impute data")
    try:
        df = pd.read_pickle(input_src)
        mi = MissingIndicator(features="all")
        columns = [f"mi__{x}" for x in df]
        df_mi = pd.DataFrame(mi.fit_transform(df), columns=columns, index=df.index)
        columns = df_mi.sum()[df_mi.sum() > 0].index.values
        df_mi = df_mi[columns]
        df = pd.concat([df, df_mi], axis=1)
        subsample = int(len(df) / 5)
        n_quantiles = min(
            1000, subsample - 1
        )  # default is 1000, use min to ensure < subsample
        qt = QuantileTransformer(
            n_quantiles=n_quantiles,
            output_distribution="normal",
            subsample=subsample,
            random_state=random_state,
        )
        imputer = SimpleImputer(strategy="median")
        pipe = Pipeline(
            steps=[
                ("quantile_transformer", qt),
                ("imputer", imputer),
                ("standard_scaler", StandardScaler()),
            ],
            memory=str(cache_dir),
            verbose=True,
        )
        df_transformed = pipe.fit_transform(df)
        df_transformed = pd.DataFrame(
            df_transformed, index=df.index, columns=df.columns
        )

        logger.debug("Finish scaling and imputing")
    except Exception:
        logger.error("Failed to scale / impute data", exc_info=True)
        raise

    # @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
    print("Save outputs")
    try:
        df_transformed.to_pickle(output_dst)
        with open(str(model_dst), "wb") as f:
            pickle.dump(pipe, f)
        logger.debug("Save outputs")
    except Exception:
        logger.error("Failed to save output(s)", exc_info=True)
        raise
