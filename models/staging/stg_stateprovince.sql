with source as (
    select
        /* Primary Key */
        stateprovinceid
                
        /* Entity attributes */
        , stateprovincecode
        , countryregioncode
        , isonlystateprovinceflag
        , name
        , territoryid
        , rowguid
        , modifieddate
        
        /* Stitch columns */
        , _sdc_table_version
        , _sdc_sequence
        , _sdc_received_at
        , _sdc_batched_at
        
    from {{ source('adventureworks_etl', 'stateprovince') }}
)
select * from source