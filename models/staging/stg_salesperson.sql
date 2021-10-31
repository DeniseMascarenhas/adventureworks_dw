with source as (
    select
        /* Primary Key */
        businessentityid
                
        /* Entity attributes */
        , territoryid
        , salesquota
        , saleslastyear
        , commissionpct
        , bonus
        , salesytd                
        , rowguid
        , modifieddate

        /* Stitch columns */
        , _sdc_table_version
        , _sdc_sequence
        , _sdc_received_at
        , _sdc_batched_at
        
    from {{ source('adventureworks_etl', 'salesperson') }}
)
select * from source