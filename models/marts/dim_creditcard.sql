with stage_selected as (
    select
    /*    distinct cardtype */
    creditcardid
    , cardnumber
    , cardtype
    , expyear
    , expmonth
    , modifieddate
        
    from {{ ref('stg_creditcard') }}
),
transformed as (
    select 
    /*    row_number() over (order by cardtype) as cardtype_sk */
        row_number() over (order by creditcardid) as creditcard_sk
        , *
    from stage_selected
)
select * from transformed