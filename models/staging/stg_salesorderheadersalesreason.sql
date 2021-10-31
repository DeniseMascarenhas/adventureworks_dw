with source as (
    select
        /* Primary Key */
        salesorderid
        , salesreasonid

        /* Entity attributes */
        , modifieddate

        /* Stitch columns */
        , _sdc_table_version
        , _sdc_sequence
        , _sdc_received_at
        , _sdc_batched_at
        
    from {{ source('adventureworks_etl', 'salesorderheadersalesreason') }}
)
select * from source