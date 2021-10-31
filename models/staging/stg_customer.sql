with source as (
    select
        /* Primary Key */
        customerid
        
        /* Entity attributes */
        , personid
        , territoryid
        , storeid
        , modifieddate
        , rowguid
                
        /* Stitch columns */
        , _sdc_table_version
        , _sdc_sequence
        , _sdc_received_at
        , _sdc_batched_at
        
    from {{ source('adventureworks_etl', 'customer') }}
)
select * from source