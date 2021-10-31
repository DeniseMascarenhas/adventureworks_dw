with
    countryregion as (
        select *
        from {{ ref('stg_countryregion') }}
    )
    , stateprovince as (
        select *
        from {{ ref('stg_stateprovince') }}
    )
    , stage_selected as (
        select
        distinct
        city
        , address.stateprovinceid
        , stateprovince.name as stateprovince_name
        , stateprovince.countryregioncode
        , countryregion.name as countryregion_name
        
        from {{ ref('stg_address') }} as address
        left join stateprovince on address.stateprovinceid = stateprovince.stateprovinceid
        left join countryregion on stateprovince.countryregioncode = countryregion.countryregioncode
),
transformed as (
    select
        row_number() over (order by city) as city_sk
        , *
    from stage_selected
)
select * from transformed