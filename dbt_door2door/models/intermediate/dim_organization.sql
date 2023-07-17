with

dim_organization as (

    select * from {{ ref('stg_door2door') }}

),

final as (

    select
        distinct
        organization_id

    from dim_organization

)

select * from final