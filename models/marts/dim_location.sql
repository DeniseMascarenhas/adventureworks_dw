with
    stage_selected as (
        select
        distinct
        city
        , address.stateprovinceid
        , stateprovincename
        , countryregioncode
        , countryregionname
        
        from {{ ref('stg_address') }} as address
),
transformed as (
    select
        row_number() over (order by city) as location_sk
        , *
    from stage_selected
)
select * from transformed