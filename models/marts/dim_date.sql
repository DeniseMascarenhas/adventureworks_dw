with stage_selected as (
    select
        distinct 
        orderdate as date
        
    from {{ ref('stg_salesorderheader') }}
),
transformed as (
    select 
        row_number() over (order by date) as date_sk
        , *
    from stage_selected
)
select * from transformed