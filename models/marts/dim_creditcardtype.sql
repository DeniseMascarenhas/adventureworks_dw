with stage_selected as (
    select
    distinct cardtype
        
    from {{ ref('stg_creditcard') }}
),
transformed as (
    select 
        row_number() over (order by cardtype) as cardtype_sk
        , *
    from stage_selected
)
select * from transformed