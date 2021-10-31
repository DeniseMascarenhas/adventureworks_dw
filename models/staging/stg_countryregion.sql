with source as (
    select
        /* Primary Key */
        countryregioncode
        
        /* Entity attributes */
        , name
        , modifieddate
        
        /* Stitch columns */
        , _sdc_table_version
        , _sdc_sequence
        , _sdc_received_at
        , _sdc_batched_at
        
    from {{ source('adventureworks_etl', 'countryregion') }}
)
select * from source