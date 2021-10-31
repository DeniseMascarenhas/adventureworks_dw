with source as (
    select
        /* Primary Key */
        addressid
        
        /* Entity attributes */
        , addressline1
        , addressline2
        , city
        , stateprovinceid
        , postalcode
        , spatiallocation
        , rowguid
        , modifieddate
        
        /* Stitch columns */
        , _sdc_table_version
        , _sdc_sequence
        , _sdc_received_at
        , _sdc_batched_at
        
    from {{ source('adventureworks_etl', 'address') }}
)
select * from source