select 
    *
    , case
    when rentals_count > 2 and purchase_year < 2022 then TRUE
    else FALSE
    end as replacement_candidate
from (
    select
        rentals.bike_id as bike_id
        , bikes.purchase_year as purchase_year
        , count(*) as rentals_count
    from {{ ref('stg_rentals_enriched') }} rentals
    left join {{ ref('stg_bikes') }} bikes
        on rentals.bike_id = bikes.bike_id
    group by rentals.bike_id, bikes.purchase_year
)
