# standard library imports
import argparse
from pathlib import Path

# third-party imports
from loguru import logger
import pandas as pd

# local imports
from settings import (
    RAW_ACS_DATA_DIR,
    INTERIM_DIR,
    ACS_SPAN,
    ACS_YEAR,
    LOOKUPS_SRC,
    PROCESSED_DIR,
)
from src.acs import ACS


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
        description = "Parse raw ACS tables and join them into one table"
        parser = argparse.ArgumentParser(description=description)
        parser.add_argument(
            "-l",
            "--lookups_input_src",
            default=LOOKUPS_SRC,
            help="Path to input file you can edit to select which ACS tables you want",
            type=Path,
        )
        parser.add_argument(
            "-r",
            "--raw_acs_data_dir",
            default=RAW_ACS_DATA_DIR,
            help="Directory to download raw ACS data",
        )
        parser.add_argument(
            "-i",
            "--interim_dir",
            default=INTERIM_DIR,
            help="Directory to save parsed ACS files",
        )
        parser.add_argument(
            "-p",
            "--processed_dir",
            default=PROCESSED_DIR,
            help="Directory to save parsed ACS files",
        )
        parser.add_argument(
            "-s",
            "--acs_span",
            default=ACS_SPAN,
            help="Specify which year of ACS data you want",
            type=int,
        )
        parser.add_argument(
            "-y",
            "--acs_year",
            default=ACS_YEAR,
            help="Specify which year of ACS data you want",
            type=int,
        )
        args = parser.parse_args()
        lookups_input_src = args.lookups_input_src
        raw_acs_data_dir = args.raw_acs_data_dir
        interim_dir = args.interim_dir
        acs_span = args.acs_span
        acs_year = args.acs_year
        processed_dir = args.processed_dir
        logger.debug("Finish parsing arguments")
    except Exception:
        logger.error("Failed to parse arguments", exc_info=True)
        raise

    # @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
    print("Get zips, geos, and lookups data")
    try:
        acs = ACS(
            acs_year,
            acs_span,
            raw_acs_data_dir,
            interim_dir,
            lookups_input_src,
            overwrite=False,
            verbose=False,
        )
        acs.get_data_zips()
        acs.get_geos()
        acs.get_lookups()
        logger.debug("Finish getting zips, geos, and lookups data")
    except Exception:
        logger.error("Failed to get zips / geos / lookups data", exc_info=True)
        raise

    # @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
    print("Parse tables")
    try:
        acs.parse_tables()
        logger.debug("Finished parsing tables")
    except Exception:
        logger.error("Failed to parse tables", exc_info=True)
        raise

    # @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
    print("Join tables")
    try:
        acs.join_tables()
        logger.debug('Joined tables')
    except Exception:
        logger.error("Failed to join tables", exc_info=True)
        raise

    # @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
    print("Preprocess tables")
    try:
        acs.preprocess_tables()
        logger.debug('Preprocessed tables')
    except Exception:
        logger.error("Failed to preprocess tables", exc_info=True)
        raise