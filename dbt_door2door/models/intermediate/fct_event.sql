{{
    config(
        materialized = 'incremental',
        on_schema_change = 'fail'
    )
}}

with

door2door as (

    select * from {{ ref('stg_door2door') }}
),

entity as (

    select * from {{ ref('dim_entity') }}

),

dim_location as (
    select * from {{ ref('dim_location') }}
),

dim_date as (
    select * from {{ ref('dim_date') }}
),

organization as (
    select * from {{ ref('dim_organization') }}
),

fct_event as 
(
    select
    {{ dbt_utils.generate_surrogate_key(['entity.entity_id', 'updated_at']) }} as event_id,
    entity.entity_id,
    door2door.updated_at,
    organization.organization_id,
    dim_location.location_id,
    door2door.operating_period_start,
    door2door.operating_period_finish,
    door2door.event_type
    
    from door2door

    inner join entity
        on door2door.entity_id = entity.entity_id

    inner join organization 
        on door2door.organization_id = organization.organization_id

    left join dim_location
        ON (door2door.entity_location_lat IS NULL AND door2door.entity_location_lng IS NULL
      AND dim_location.location_lat IS NULL AND dim_location.location_lng IS NULL)
      OR (door2door.entity_location_lat IS NOT NULL AND door2door.entity_location_lng IS NOT NULL
          AND door2door.entity_location_lat = dim_location.location_lat AND door2door.entity_location_lng = dim_location.location_lng) 

    left join dim_date
        on DATE(door2door.updated_at) = DATE(dim_date.date_day)

)

select * from fct_event
{% if is_incremental() %}

  where updated_at > (select max(updated_at) from {{ this }})

{% endif %}
order by updated_at