select
    source_name,
    source_id,
    name,
    category,
    area,
    instructions,
    thumbnail
from {{ source('recipe_dw', 'recipes') }}