with source as (
    select
        /* Primary Key */
        salesreasonid
        
        /* Entity attributes */
        , name
        , reasontype
        , modifieddate
                
        /* Stitch columns */
        , _sdc_table_version
        , _sdc_sequence
        , _sdc_received_at
        , _sdc_batched_at
        
    from {{ source('adventureworks_etl', 'salesreason') }}
)
select * from source