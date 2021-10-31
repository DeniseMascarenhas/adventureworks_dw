with stage_selected as (
    select
        stateprovinceid
        , name
        , stateprovincecode
        , countryregioncode 
        , isonlystateprovinceflag
        , territoryid
        
    from {{ ref('stg_stateprovince') }}
),
transformed as (
    select 
        row_number() over (order by stateprovinceid) as stateprovince_sk
        , *
    from stage_selected
)
select * from transformed