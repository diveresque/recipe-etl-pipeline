{{ config(
    materialized='view'
) }}

WITH source AS (
    SELECT
        source_name,
        source_id,
        ingredients
    FROM {{ source('recipe_dw', 'recipes') }}
),

unnested AS (
    SELECT
        source_name,
        source_id,
        ing.element.ingredient AS ingredient_name,
        ing.element.measure AS measure
    FROM source,
    UNNEST(ingredients.list) AS ing
)

SELECT
    -- stable natural key (important later)
    CONCAT(source_name, ':', source_id) AS recipe_nk,
    source_name,
    source_id,
    ingredient_name,
    measure
FROM unnested
WHERE ingredient_name IS NOT NULL
