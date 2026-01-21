{{ config(materialized='table') }}

select distinct
  {{ dbt_utils.generate_surrogate_key(['ingredient_name']) }} as ingredient_key,
  ingredient_name
from {{ ref('stg_ingredients') }}
