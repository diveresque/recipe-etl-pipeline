# src/extract/fetch_recipes.py
import json
import logging
import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import requests
from dotenv import load_dotenv, find_dotenv
from requests.adapters import HTTPAdapter, Retry

load_dotenv(find_dotenv(), override=False)

logger = logging.getLogger(__name__)

RAW_DIR = Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)

THEMEALDB_DEFAULT_CATEGORIES = ["Dessert", "Breakfast"]
SPOONACULAR_DEFAULT_TYPES = ["dessert", "breakfast"]


def _requests_session() -> requests.Session:
    retry = Retry(
        total=5,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def _slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")


def _cache_path(source: str, label: str) -> Path:
    path = RAW_DIR / source / f"{_slug(label)}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _load_cache(path: Path) -> Optional[List[Dict]]:
    if path.exists():
        with open(path, "r", encoding="utf8") as f:
            return json.load(f)
    return None


def _save_cache(path: Path, data: List[Dict]) -> None:
    with open(path, "w", encoding="utf8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _normalize_themealdb(meal: Dict) -> Dict:
    ingredients: List[Dict] = []
    for i in range(1, 21):
        ingredient = meal.get(f"strIngredient{i}")
        measure = meal.get(f"strMeasure{i}")
        if ingredient and ingredient.strip():
            ingredients.append(
                {
                    "ingredient": ingredient.strip(),
                    "measure": measure.strip() if measure else None,
                }
            )

    return {
        "source_name": "themealdb",
        "source_id": meal.get("idMeal"),
        "name": meal.get("strMeal"),
        "category": meal.get("strCategory"),
        "area": meal.get("strArea"),
        "instructions": meal.get("strInstructions"),
        "thumbnail": meal.get("strMealThumb"),
        "ingredients": ingredients,
    }


def _fetch_themealdb_by_category(session: requests.Session, category: str) -> List[Dict]:
    url = "https://www.themealdb.com/api/json/v1/1/filter.php"
    response = session.get(url, params={"c": category}, timeout=20)
    response.raise_for_status()
    data = response.json()
    return data.get("meals", []) or []


def _fetch_themealdb_detail(session: requests.Session, meal_id: str) -> Optional[Dict]:
    url = "https://www.themealdb.com/api/json/v1/1/lookup.php"
    response = session.get(url, params={"i": meal_id}, timeout=20)
    response.raise_for_status()
    data = response.json()
    meals = data.get("meals") or []
    if meals:
        return meals[0]
    return None


def fetch_themealdb(
    categories: Optional[List[str]] = None,
    refresh: bool = False,
    session: Optional[requests.Session] = None,
) -> List[Dict]:
    categories = categories or THEMEALDB_DEFAULT_CATEGORIES
    session = session or _requests_session()

    records: List[Dict] = []
    for category in categories:
        cache = _load_cache(_cache_path("themealdb", category)) if not refresh else None

        if cache is None:
            logger.info("Fetching TheMealDB category '%s' from API", category)
            meals = _fetch_themealdb_by_category(session, category)
            normalized_for_category: List[Dict] = []
            unique_ids = {meal.get("idMeal") for meal in meals if meal.get("idMeal")}
            for meal_id in unique_ids:
                detail = _fetch_themealdb_detail(session, meal_id)
                if detail:
                    normalized_for_category.append(_normalize_themealdb(detail))
                    time.sleep(0.1)  # polite pacing
                else:
                    logger.warning(
                        "No detail returned for TheMealDB meal id=%s in category '%s'",
                        meal_id,
                        category,
                    )
            _save_cache(_cache_path("themealdb", category), normalized_for_category)
            cache = normalized_for_category
            logger.info(
                "Cached %d TheMealDB recipes for category '%s'",
                len(cache),
                category,
            )
        else:
            logger.info(
                "Loaded TheMealDB category '%s' from cache (%d recipes)",
                category,
                len(cache),
            )

        records.extend(cache)

    return records


def _build_instruction_text(recipe: Dict) -> Optional[str]:
    instructions = recipe.get("instructions")
    if instructions:
        return instructions

    analyzed = recipe.get("analyzedInstructions") or []
    steps: List[str] = []
    for section in analyzed:
        for step in section.get("steps") or []:
            text = step.get("step")
            if text:
                steps.append(text.strip())
    if steps:
        return "\n".join(steps)
    return None


def _format_spoonacular_measure(item: Dict) -> Optional[str]:
    measures = item.get("measures") or {}
    for system in ("metric", "us"):
        data = measures.get(system) or {}
        amount = data.get("amount")
        unit = data.get("unitShort") or data.get("unitLong")
        if amount is not None:
            amount_str = f"{amount:g}" if isinstance(amount, (int, float)) else str(amount)
            return f"{amount_str} {unit}".strip() if unit else amount_str
    return item.get("originalString") or item.get("originalName") or item.get("original")


def _normalize_spoonacular(recipe: Dict, dish_type: str) -> Dict:
    ingredients: List[Dict] = []
    for item in recipe.get("extendedIngredients") or []:
        name = (
            item.get("nameClean")
            or item.get("name")
            or item.get("originalName")
            or item.get("original")
        )
        measure = _format_spoonacular_measure(item)
        if name:
            ingredients.append({"ingredient": name, "measure": measure})

    cuisines = recipe.get("cuisines") or []
    area = ", ".join(cuisines) if cuisines else None
    category = dish_type.title() if dish_type else None

    return {
        "source_name": "spoonacular",
        "source_id": str(recipe.get("id")),
        "name": recipe.get("title"),
        "category": category,
        "area": area,
        "instructions": _build_instruction_text(recipe),
        "thumbnail": recipe.get("image"),
        "ingredients": ingredients,
    }


def _fetch_spoonacular_bulk(
    session: requests.Session, recipe_ids: List[str], api_key: str
) -> List[Dict]:
    """Fetch full recipe details for multiple recipes in one bulk call."""
    # Spoonacular bulk endpoint accepts comma-separated IDs
    ids_param = ",".join(recipe_ids)
    url = "https://api.spoonacular.com/recipes/informationBulk"
    params = {"ids": ids_param, "apiKey": api_key}
    response = session.get(url, params=params, timeout=60)  # Longer timeout for bulk
    response.raise_for_status()
    return response.json()


def fetch_spoonacular(
    dish_types: Optional[List[str]] = None,
    refresh: bool = False,
    session: Optional[requests.Session] = None,
) -> List[Dict]:
    dish_types = dish_types or SPOONACULAR_DEFAULT_TYPES
    api_key = os.getenv("SPOONACULAR_API_KEY")
    if not api_key:
        logger.warning("SPOONACULAR_API_KEY not set; skipping Spoonacular fetch.")
        return []

    session = session or _requests_session()
    search_url = "https://api.spoonacular.com/recipes/complexSearch"

    records: List[Dict] = []
    for dish_type in dish_types:
        cache = _load_cache(_cache_path("spoonacular", dish_type)) if not refresh else None

        if cache is None:
            logger.info("Fetching Spoonacular dish type '%s' from API", dish_type)
            # First, get recipe IDs from search
            params = {
                "apiKey": api_key,
                "type": dish_type,
                "instructionsRequired": "true",
                "number": 100,
            }
            response = session.get(search_url, params=params, timeout=30)
            response.raise_for_status()
            payload = response.json()
            results = payload.get("results") or []
            
            # Get unique recipe IDs
            unique_ids = [str(r.get("id")) for r in results if r.get("id")]
            logger.info(
                "Found %d recipe IDs for '%s', fetching full details via bulk endpoint...",
                len(unique_ids),
                dish_type,
            )
            
            # Fetch full details using bulk endpoint (much more efficient!)
            normalized: List[Dict] = []
            if unique_ids:
                # Bulk endpoint can handle up to 100 IDs at once
                # If we have more, we'd need to batch, but complexSearch limits to 100 anyway
                try:
                    bulk_details = _fetch_spoonacular_bulk(session, unique_ids, api_key)
                    for detail in bulk_details:
                        if detail:
                            normalized.append(_normalize_spoonacular(detail, dish_type))
                    logger.info(
                        "Successfully fetched %d full recipe details for '%s'",
                        len(normalized),
                        dish_type,
                    )
                except Exception as e:
                    logger.error(
                        "Error fetching bulk details for '%s': %s. (Optional) Falling back to individual calls.",
                        dish_type,
                        e,
                    )
                    # Fallback: individual calls if bulk fails
                    # for recipe_id in unique_ids:
                    # This is the individual call fallback, but it's not used in the code because of rate limits to Spoonacular API
                    #
                    #     url = f"https://api.spoonacular.com/recipes/{recipe_id}/information"
                    #     try:
                    #         response = session.get(url, params={"apiKey": api_key}, timeout=30)
                    #         response.raise_for_status()
                    #         detail = response.json()
                    #         if detail:
                    #             normalized.append(_normalize_spoonacular(detail, dish_type))
                    #         time.sleep(0.1)  # polite pacing
                    #     except Exception as err:
                    #         logger.warning(
                    #             "Failed to fetch recipe id=%s: %s", recipe_id, err
                    #         )
            
            _save_cache(_cache_path("spoonacular", dish_type), normalized)
            cache = normalized
            logger.info(
                "Cached %d Spoonacular recipes for dish type '%s'",
                len(cache),
                dish_type,
            )
        else:
            logger.info(
                "Loaded Spoonacular dish type '%s' from cache (%d recipes)",
                dish_type,
                len(cache),
            )

        records.extend(cache)

    return records


def save_raw(filename: str, obj: List[Dict]) -> Path:
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    path = RAW_DIR / f"{filename}_{ts}.json"
    with open(path, "w", encoding="utf8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    return path


def fetch_and_save(
    themealdb_categories: Optional[List[str]] = None,
    spoonacular_types: Optional[List[str]] = None,
    refresh: bool = False,
) -> str:
    session = _requests_session()

    records: Dict[tuple, Dict] = {}

    for record in fetch_themealdb(
        categories=themealdb_categories, refresh=refresh, session=session
    ):
        key = (record["source_name"], record["source_id"])
        records.setdefault(key, record)

    for record in fetch_spoonacular(
        dish_types=spoonacular_types, refresh=refresh, session=session
    ):
        key = (record["source_name"], record["source_id"])
        records.setdefault(key, record)

    combined = list(records.values())
    saved = save_raw("recipes_initial", combined)
    logger.info("Saved %d recipes to raw file %s", len(combined), saved)
    return str(saved)


if __name__ == "__main__":
    fetch_and_save()