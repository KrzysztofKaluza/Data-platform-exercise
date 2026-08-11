select *
from {{ source('silver', 'bikes') }}