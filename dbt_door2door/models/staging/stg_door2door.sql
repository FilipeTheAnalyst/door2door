with

door2door as (

    select * from {{ source('door2door', 'raw_data') }}

),

final as (

    select
        event as event_type,
        d.on as entity_type,
        TIMESTAMP(created_at) as updated_at,
        organization_id,
        id as entity_id,
        location_lat as entity_location_lat,
        location_lng as entity_location_lng,
        TIMESTAMP(location_created_at) as entity_location_updated_at,
        TIMESTAMP(start) as operating_period_start,
        TIMESTAMP(finish) as operating_period_finish

    from door2door d

)

select * from final