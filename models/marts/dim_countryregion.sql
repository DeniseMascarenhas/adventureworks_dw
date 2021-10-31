with stage_selected as (
    select
        countryregioncode
        , name
        
    from {{ ref('stg_countryregion') }}
    order by countryregioncode
),
transformed as (
    select 
        row_number() over (order by countryregioncode) as countryregion_sk
        , *
    from stage_selected
)
select * from transformed