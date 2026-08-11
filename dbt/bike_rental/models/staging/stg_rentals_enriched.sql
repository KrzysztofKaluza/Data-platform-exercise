select *
from {{ source('silver', 'rentals_enriched') }}