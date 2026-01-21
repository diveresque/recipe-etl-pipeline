# src/load/load_to_db.py
import logging
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from src.utils.db import engine

logger = logging.getLogger(__name__)

PROC_DIR = Path("data/processed")


def create_tables_if_not_exists():
    create_recipes = """
    CREATE TABLE IF NOT EXISTS recipes (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      source_name VARCHAR(64) NOT NULL,
      source_id VARCHAR(64) NOT NULL,
      name TEXT,
      category VARCHAR(255),
      area VARCHAR(255),
      instructions LONGTEXT,
      thumbnail TEXT,
      UNIQUE KEY uq_recipes_source (source_name, source_id)
    ) CHARACTER SET utf8mb4;
    """
    create_ingredients = """
    CREATE TABLE IF NOT EXISTS ingredients (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      name VARCHAR(255),
      normalized_name VARCHAR(255) NOT NULL UNIQUE
    ) CHARACTER SET utf8mb4;
    """

    create_recipe_ingredients = """
    CREATE TABLE IF NOT EXISTS recipe_ingredients (
        recipe_id BIGINT NOT NULL,
        ingredient_id BIGINT NOT NULL,
        measure VARCHAR(255),
        PRIMARY KEY (recipe_id, ingredient_id),
        FOREIGN KEY (recipe_id) REFERENCES recipes(id) ON DELETE CASCADE,
        FOREIGN KEY (ingredient_id) REFERENCES ingredients(id) ON DELETE CASCADE
    ) CHARACTER SET utf8mb4;
    """

    with engine.begin() as conn:
        conn.execute(text(create_recipes))
        conn.execute(text(create_ingredients))
        conn.execute(text(create_recipe_ingredients))
    logger.info("Ensured recipes, ingredients, and recipe_ingredients tables exist.")

def load_parquet_to_db(path=None):
    """
    Load parquet data into the database.
    
    Returns:
        dict with keys: recipes_loaded, ingredients_loaded, mappings_loaded
    """
    if path is None:
        path = PROC_DIR / "recipes.parquet"
    df = pd.read_parquet(path)
    if df.empty:
        logger.warning("Processed DataFrame %s is empty; nothing to load.", path)
        return {"recipes_loaded": 0, "ingredients_loaded": 0, "mappings_loaded": 0}

    # Prepare recipes dataframe with source metadata already provided upstream
    recipes_df = (
        df[['source_name','source_id','name','category','area','instructions','thumbnail']]
        .drop_duplicates(subset=['source_name','source_id'])
    )
    logger.info("Preparing to upsert %d recipes from %s", len(recipes_df), path)
    # Use pandas to_sql to write to a staging table, then do upsert to main tables.
    # 1) write recipes into a temp table
    with engine.begin() as conn:
        recipes_df.to_sql('recipes_stage', conn, if_exists='replace', index=False, chunksize=1000)
        # upsert from recipes_stage into recipes table using INSERT ... ON DUPLICATE KEY UPDATE against unique (source_name, source_id)
        upsert_recipes = """
        INSERT INTO recipes (source_name, source_id, name, category, area, instructions, thumbnail)
        SELECT source_name, source_id, name, category, area, instructions, thumbnail FROM recipes_stage
        ON DUPLICATE KEY UPDATE
          name = VALUES(name),
          category = VALUES(category),
          area = VALUES(area),
          instructions = VALUES(instructions),
          thumbnail = VALUES(thumbnail);
        """
        conn.execute(text(upsert_recipes))
        conn.execute(text("DROP TABLE IF EXISTS recipes_stage"))
    logger.info("Upserted %d recipes into database", len(recipes_df))

    # ---------------------------------------------------------
    # 2) Build UNIQUE INGREDIENT LIST from all recipes
    # ---------------------------------------------------------
    all_ing_rows = []

    for _, row in df.iterrows():
        ingredients_array = row["ingredients"]
        if isinstance(ingredients_array, list):
            ing_list = ingredients_array
        else:
            ing_list = ingredients_array.tolist()

        for ing in ing_list:
            if not ing.get("ingredient"):
                continue

            name = ing["ingredient"].strip()
            normalized = name.lower().strip()

            all_ing_rows.append({"name": name, "normalized_name": normalized})

    ing_df = pd.DataFrame(all_ing_rows).drop_duplicates(subset=["normalized_name"])

    logger.info("Extracted %d distinct ingredients.", len(ing_df))

    # Insert ingredients into DB (deduping via UNIQUE(normalized_name))
    with engine.begin() as conn:
        ing_df.to_sql("ingredients_stage", conn, if_exists="replace", index=False)

        upsert_ing = """
        INSERT INTO ingredients (name, normalized_name)
        SELECT name, normalized_name FROM ingredients_stage
        ON DUPLICATE KEY UPDATE
            name = VALUES(name);
        """
        conn.execute(text(upsert_ing))
        conn.execute(text("DROP TABLE IF EXISTS ingredients_stage"))

    logger.info("Upserted %d ingredients.", len(ing_df))

    # ---------------------------------------------------------
    # 3) Build recipe_ingredients mapping table
    # ---------------------------------------------------------
    mapping_rows = []

    for _, row in df.iterrows():
        source_name = row["source_name"]
        source_id = row["source_id"]

        # Convert ingredients
        ingredients_array = row["ingredients"]
        ing_list = (
            ingredients_array if isinstance(ingredients_array, list) else ingredients_array.tolist()
        )

        for ing in ing_list:
            if not ing.get("ingredient"):
                continue

            name = ing["ingredient"].strip()
            normalized = name.lower().strip()

            mapping_rows.append({
                "source_name": source_name,
                "source_id": source_id,
                "normalized_name": normalized,
                "measure": ing.get("measure"),
            })

    map_df = pd.DataFrame(mapping_rows)
    logger.info("Prepared %d recipe-ingredient mapping records.", len(map_df))

    with engine.begin() as conn:
        map_df.to_sql("recipe_ing_stage", conn, if_exists="replace", index=False)

        insert_mapping = """
        INSERT INTO recipe_ingredients (recipe_id, ingredient_id, measure)
        SELECT r.id, i.id, s.measure
        FROM recipe_ing_stage s
        JOIN recipes r
          ON r.source_name = s.source_name
         AND r.source_id = s.source_id
        JOIN ingredients i
          ON i.normalized_name = s.normalized_name
        ON DUPLICATE KEY UPDATE
            measure = VALUES(measure);
        """
        conn.execute(text(insert_mapping))
        conn.execute(text("DROP TABLE IF EXISTS recipe_ing_stage"))

    logger.info("Upserted recipe–ingredient mappings.")

    logger.info("LOAD COMPLETED SUCCESSFULLY.")
    
    # Get actual counts from database
    with engine.begin() as conn:
        recipe_count = conn.execute(text("SELECT COUNT(*) FROM recipes")).scalar()
        ingredient_count = conn.execute(text("SELECT COUNT(*) FROM ingredients")).scalar()
        mapping_count = conn.execute(text("SELECT COUNT(*) FROM recipe_ingredients")).scalar()
    
    return {
        "recipes_loaded": recipe_count,
        "ingredients_loaded": ingredient_count,
        "mappings_loaded": mapping_count,
    }

if __name__ == "__main__":
    create_tables_if_not_exists()
    load_parquet_to_db()
