with
    salesperson as (
        select *
        from {{ ref('dim_salesperson') }}        
    )
    , customer as (
        select *
        from {{ ref('dim_customer') }}
    )
    , creditcardtype as (
        select *
        from {{ ref('dim_creditcardtype') }}
    )
    , creditcard_with_type_sk as (
        select 
            creditcardid
            , cardtype_sk
        from {{ ref('stg_creditcard') }} as creditcard
        left join creditcardtype on creditcard.cardtype = creditcardtype.cardtype
    )
    , product as (
        select *
        from {{ ref('dim_product') }}
    )    
    , location as (
        select *
        from {{ ref('dim_location') }}
    )
    , ship_address_with_city_sk as (
        select
            addressid
            , city_sk
    
        from {{ ref('stg_address') }} as address
        left join location on address.city = location.city
    )
    , order_with_sk as (
        select
            salesorderheader.salesorderid
            , salesperson.businessentityid as salesperson_fk
            , customer.customer_sk as customer_fk
            , creditcard_with_type_sk.creditcardid            
            , creditcard_with_type_sk.cardtype_sk as cardtype_fk
            , salesorderheader.shiptoaddressid
            , ship_address_with_city_sk.city_sk as ship_city_fk
            , salesorderheader.orderdate
            , salesorderheader.duedate
            , salesorderheader.shipdate
            , salesorderheader.status
            , salesorderheader.territoryid
            , salesorderheader.shipmethodid
            , salesorderheader.currencyrateid
            , salesorderheader.creditcardapprovalcode
            , salesorderheader.purchaseordernumber
            , salesorderheader.accountnumber
            , salesorderheader.revisionnumber
            , salesorderheader.onlineorderflag
            , salesorderheader.taxamt
            , salesorderheader.subtotal
            , salesorderheader.freight
            , salesorderheader.totaldue

        from {{ ref('stg_salesorderheader') }} as salesorderheader
        left join salesperson                on salesorderheader.salespersonid   = salesperson.businessentityid
        left join customer                   on salesorderheader.customerid      = customer.customerid
        left join creditcard_with_type_sk    on salesorderheader.creditcardid    = creditcard_with_type_sk.creditcardid
        left join ship_address_with_city_sk  on salesorderheader.shiptoaddressid = ship_address_with_city_sk.addressid
    )
    , order_detail_with_sk as (
        select
            salesorderid
            , salesorderdetailid
            , product_sk as product_fk
            , orderqty
            , unitprice
            , unitpricediscount
    
        from {{ ref('stg_salesorderdetail') }} as salesorderdetail
        left join product on salesorderdetail.productid = product.productid
    )
    , final as (
        select
            order_detail_with_sk.salesorderid
            , order_with_sk.salesperson_fk
            , order_with_sk.customer_fk
            , order_with_sk.creditcardid
            , order_with_sk.cardtype_fk
            , order_with_sk.orderdate
            , order_with_sk.duedate
            , order_with_sk.shipdate
            , order_with_sk.status
            , order_with_sk.shiptoaddressid
            , order_with_sk.ship_city_fk
            , order_with_sk.shipmethodid
            , order_with_sk.currencyrateid
            , order_with_sk.creditcardapprovalcode
            , order_with_sk.purchaseordernumber
            , order_with_sk.accountnumber
            , order_with_sk.revisionnumber
            , order_with_sk.onlineorderflag
            , order_with_sk.taxamt
            , order_with_sk.subtotal
            , order_with_sk.freight
            , order_with_sk.totaldue
            , order_detail_with_sk.salesorderdetailid
            , order_detail_with_sk.product_fk
            , order_detail_with_sk.orderqty
            , order_detail_with_sk.unitprice
            , order_detail_with_sk.unitpricediscount

        from order_with_sk
        left join order_detail_with_sk on order_with_sk.salesorderid = order_detail_with_sk.salesorderid
    )
select * from final