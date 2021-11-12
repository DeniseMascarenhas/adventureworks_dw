with
    date as (
        select *
        from {{ ref('dim_date') }}
    )
    , status as (
        select *
        from {{ ref('dim_status') }}
    )
    , customer as (
        select *
        from {{ ref('dim_customer') }}
    )
    , cardtype as (
        select *
        from {{ ref('dim_cardtype') }}
    )
    , creditcard_with_type_sk as (
        select 
            creditcardid
            , cardtype_sk
        from {{ ref('stg_creditcard') }} as creditcard
        left join cardtype on creditcard.cardtype = cardtype.cardtype
    )
    , product as (
        select *
        from {{ ref('dim_product') }}
    )
    , location as (
        select *
        from {{ ref('dim_location') }}
    )    
    , ship_address_with_location_sk as (
        select
            addressid
            , location_sk
    
        from {{ ref('stg_address') }} as address
        left join location on address.city = location.city
                          and address.stateprovinceid = location.stateprovinceid
    )
    , order_with_sk as (
        select
            salesorderheader.salesorderid
            , customer.customer_sk as customer_fk
            , creditcard_with_type_sk.creditcardid          
            , creditcard_with_type_sk.cardtype_sk as cardtype_fk
            , salesorderheader.shiptoaddressid
            , ship_address_with_location_sk.location_sk as shiplocation_fk
            , status.status_sk as status_fk
            , date.date_sk as orderdate_fk
            , salesorderheader.duedate
            , salesorderheader.shipdate
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
        left join customer                       on salesorderheader.customerid      = customer.customerid
        left join creditcard_with_type_sk        on salesorderheader.creditcardid    = creditcard_with_type_sk.creditcardid
        left join ship_address_with_location_sk  on salesorderheader.shiptoaddressid = ship_address_with_location_sk.addressid
        left join status                         on salesorderheader.status          = status.status
        left join date                           on cast(salesorderheader.orderdate as date) = date.date_sk
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
            , order_with_sk.customer_fk
            , order_with_sk.cardtype_fk
            , order_with_sk.status_fk
            , order_with_sk.orderdate_fk
            , order_with_sk.shiplocation_fk
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
    , transformed as (
        select
            row_number() over (order by salesorderid, salesorderdetailid) as salesorder_sk
            , *
        from final
    )
select * from transformed