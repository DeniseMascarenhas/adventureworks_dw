with stage_selected as (
    select
        salesreasonid 
        , name as salesreasonname
        , reasontype as reasontype
        
    from {{ ref('stg_salesreason') }}
),
transformed as (
    select 
        row_number() over (order by salesreasonid) as salesreason_sk
        , *
    from stage_selected
)
select * from transformed