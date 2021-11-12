with stage_selected as (
    select
        distinct 
        orderdate as date
        
    from {{ ref('stg_salesorderheader') }}
),
transformed as (
    select 
        cast(date as date) as date_sk
        , extract(year FROM date) as year
        , extract(month FROM date) as month
        , extract(day FROM date) as day
        , extract(year from date) * 100 + extract(month from date) as year_month        
        
    from stage_selected
)
select * from transformed