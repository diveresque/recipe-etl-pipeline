SELECT
  {{ dbt_utils.generate_surrogate_key(['source_name', 'source_id']) }} AS recipe_key,
  source_name,
  source_id,
  name,
  category,
  area,
  instructions
  -- NEW COLUMN to test CI
  length(name) as name_length
FROM {{ ref('stg_recipes') }}
