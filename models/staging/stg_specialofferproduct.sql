with source as (
    select
        /* Primary Key */
        specialofferid
        , productid
        
        /* Entity attributes */
        , modifieddate
        , rowguid

        /* Stitch columns */
        , _sdc_table_version
        , _sdc_sequence
        , _sdc_received_at
        , _sdc_batched_at
        
    from {{ source('adventureworks_etl', 'specialofferproduct') }}
)
select * from source