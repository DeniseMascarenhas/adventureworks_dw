with source as (
    select
        /* Primary Key */
        salesorderid
        , salespersonid
        
        /* Entity attributes */
        , customerid
        , orderdate
        , duedate
        , shipdate
        , status
        , creditcardid
        , territoryid
        , billtoaddressid
        , shiptoaddressid
        , shipmethodid
        , currencyrateid
        , creditcardapprovalcode
        , purchaseordernumber
        , accountnumber
        , revisionnumber
        , onlineorderflag
        , taxamt
        , subtotal
        , freight
        , totaldue
        , rowguid
        , modifieddate

        /* Stitch columns */
        , _sdc_table_version
        , _sdc_sequence
        , _sdc_received_at
        , _sdc_batched_at
        
    from {{ source('adventureworks_etl', 'salesorderheader') }}
)
select * from source