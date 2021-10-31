with source as (
    select
        /* Primary Key */
        salesorderid
        , salesorderdetailid
                
        /* Entity attributes */
        , carriertrackingnumber
        , productid
        , orderqty
        , unitprice
        , unitpricediscount
        , specialofferid
        , modifieddate
        , rowguid

        /* Stitch columns */
        , _sdc_table_version
        , _sdc_sequence
        , _sdc_received_at
        , _sdc_batched_at
        
    from {{ source('adventureworks_etl', 'salesorderdetail') }}
)
select * from source