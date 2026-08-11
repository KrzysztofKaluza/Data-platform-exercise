SELECT  start_station_name as station_name,
    count(*) as rentals_count,
    avg(rental_duration_minutes) as avg_duration
from {{ ref('stg_rentals_enriched')}}
group by 1