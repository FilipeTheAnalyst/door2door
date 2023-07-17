with

dim_location as (

    select * from {{ ref('stg_door2door') }}

),

final as (

    select
        distinct
        {{ dbt_utils.generate_surrogate_key(['entity_location_lat', 'entity_location_lng']) }} as location_id,
        entity_location_lat as location_lat,
        entity_location_lng as location_lng

    from dim_location
)

select * from final