SELECT
    customer_city
    , count(*) as rentals_count
    , avg(rental_duration_minutes) as avg_duration
FROM {{ ref('stg_rentals_enriched')}}
group by customer_city