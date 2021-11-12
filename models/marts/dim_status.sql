with stage_selected as (
    select
    distinct status
        
    from {{ ref('stg_salesorderheader') }}
),
transformed as (
    select
        row_number() over (order by status) as status_sk
        , *
    from stage_selected
)
select * from transformed

