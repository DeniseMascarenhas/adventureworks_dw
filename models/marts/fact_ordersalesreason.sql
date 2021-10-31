with
    salesreason as (
        select *
        from {{ ref('dim_salesreason') }}        
    )
    , order_detail as (
        select *
        from {{ ref('fact_orderdetail') }}        
    )
    , ordersalesreason as (
        select
            order_detail.salesorderid as salesorderid
            , salesreason.salesreasonid
            
        from {{ ref('stg_salesorderheadersalesreason') }} as salesorderheadersalesreason
        left join salesreason on salesorderheadersalesreason.salesreasonid = salesreason.salesreasonid
        left join order_detail on salesorderheadersalesreason.salesorderid = order_detail.salesorderid  
    )    
select * from ordersalesreason