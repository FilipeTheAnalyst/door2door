with

dim_entity as (

    select * from {{ ref('stg_door2door') }}

),

final as (

    select
        distinct
        entity_id,
        entity_type

    from dim_entity

)

select * from final