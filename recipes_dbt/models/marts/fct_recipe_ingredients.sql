{{ config(
    materialized='incremental',
    unique_key=['recipe_key', 'ingredient_key']
) }}

select
  r.recipe_key,
  i.ingredient_key,
  s.measure,
  current_timestamp() as inserted_at
from {{ ref('stg_ingredients') }} s
join {{ ref('dim_recipes') }} r
  on s.source_name = r.source_name
 and s.source_id   = r.source_id
join {{ ref('dim_ingredients') }} i
  on s.ingredient_name = i.ingredient_name

{% if is_incremental() %}
-- Only insert rows that do not already exist
where not exists (
    select 1
    from {{ this }} t
    where t.recipe_key = r.recipe_key
      and t.ingredient_key = i.ingredient_key
)
{% endif %}
