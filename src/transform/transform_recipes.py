# src/transform/transform_recipes.py
import json
import logging
import re
from pathlib import Path
from typing import Dict, List, Optional

from src.transform.ingredient_normalizer import normalize_ingredient_name, normalize_measure
from src.quality.data_quality import DataQualityChecker

import pandas as pd

logger = logging.getLogger(__name__)

RAW_DIR = Path("data/raw")
PROC_DIR = Path("data/processed")
PROC_DIR.mkdir(parents=True, exist_ok=True)


def load_raw(latest_filename: Optional[str] = None) -> List[Dict]:
    files = sorted(RAW_DIR.glob("recipes_initial_*.json"))
    if latest_filename is not None:
        path = Path(latest_filename)
        if not path.exists():
            raise FileNotFoundError(f"Specified raw file not found: {path}")
    else:
        if not files:
            raise FileNotFoundError("No raw files found in data/raw")
        path = files[-1]
    with open(path, "r", encoding="utf8") as f:
        return json.load(f)

def flatten(recipes_json: List[Dict]) -> pd.DataFrame:
    if not recipes_json:
        logger.warning("No recipes supplied to flatten; returning empty DataFrame")
        return pd.DataFrame(
            columns=[
                "source_name",
                "source_id",
                "name",
                "category",
                "area",
                "instructions",
                "thumbnail",
                "ingredients",
            ]
        )

    rows: List[Dict] = []
    seen = set()
    for record in recipes_json:
        key = (record.get("source_name"), record.get("source_id"))
        if None in key or key in seen:
            if None in key:
                logger.warning("Skipping recipe with missing source metadata: %s", record)
            else:
                logger.debug("Skipping duplicate recipe %s", key)
            continue
        seen.add(key)

        ingredients = []
        for item in record.get("ingredients") or []:
            ingredient_name = normalize_ingredient_name(item.get("ingredient"))
            measure = item.get("measure")
            if ingredient_name:
                ingredients.append(
                    {
                        "ingredient": ingredient_name,
                        "measure": normalize_measure(measure) if isinstance(measure, str) else measure,
                    }
                )

        rows.append(
            {
                "source_name": record.get("source_name"),
                "source_id": str(record.get("source_id")),
                "name": record.get("name"),
                "category": record.get("category"),
                "area": record.get("area"),
                "instructions": record.get("instructions"),
                "thumbnail": record.get("thumbnail"),
                "ingredients": ingredients,
            }
        )

    df = pd.DataFrame(rows)
    logger.info("Flattened %d recipes from %d raw records", len(df), len(recipes_json))
    return df


def save_parquet(df: pd.DataFrame, filename: str = "recipes.parquet") -> Path:
    path = PROC_DIR / filename
    try:
        df.to_parquet(path, index=False)
    except ImportError as exc:
        raise RuntimeError("pyarrow or fastparquet is required for Parquet output") from exc
    logger.info("Saved processed parquet to %s containing %d recipes", path, len(df))
    return path


def validate_data_quality(df: pd.DataFrame, raise_on_failure: bool = False) -> DataQualityChecker:
    """
    Run data quality checks on the transformed DataFrame.
    
    Args:
        df: DataFrame to validate
        raise_on_failure: If True, raise ValueError when quality checks fail
    
    Returns:
        DataQualityChecker instance with results
    """
    checker = DataQualityChecker()
    checker.check_recipe_data_quality(df)
    checker.log_summary()
    
    if raise_on_failure:
        checker.assert_all_passed(raise_on_failure=True)
    
    return checker


def process_raw(latest_filename: Optional[str] = None, validate: bool = True, 
                raise_on_quality_failure: bool = False) -> Path:
    """
    Process raw recipe data through the transform pipeline.
    
    Args:
        latest_filename: Optional path to specific raw file to process
        validate: If True, run data quality checks
        raise_on_quality_failure: If True, raise error when quality checks fail
    
    Returns:
        Path to saved parquet file
    """
    raw = load_raw(latest_filename)
    df = flatten(raw)
    
    if validate:
        validate_data_quality(df, raise_on_failure=raise_on_quality_failure)
    
    return save_parquet(df)


if __name__ == "__main__":
    process_raw()
