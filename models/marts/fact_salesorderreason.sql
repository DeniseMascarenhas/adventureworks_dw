with
    salesreason as (
        select *
        from {{ ref('dim_salesreason') }}
    )
    , salesorder as (
        select *
        from {{ ref('fact_salesorder') }}        
    )
    , salesorderreason as (
        select
            salesorder.salesorder_sk as salesorder_fk
            , salesorder.salesorderid as salesorderid
            , salesorder.salesorderdetailid as salesorderdetailid
            , salesreason.salesreason_sk as salesreason_fk
        
        from {{ ref('stg_salesorderheadersalesreason') }} as salesorderheadersalesreason
        left join salesreason on salesorderheadersalesreason.salesreasonid = salesreason.salesreasonid
        left join salesorder on salesorderheadersalesreason.salesorderid = salesorder.salesorderid
    )
select * from salesorderreason