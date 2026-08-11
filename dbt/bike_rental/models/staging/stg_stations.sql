select *
from {{ source('silver', 'stations') }}