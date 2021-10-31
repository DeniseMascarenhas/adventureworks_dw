with source as (
    select
        /* Primary Key */
        specialofferid
                
        /* Entity attributes */
        , startdate
        , maxqty
        , modifieddate
        , rowguid
        , type
        , discountpct
        , category
        , description
        , minqty
        , enddate

        /* Stitch columns */
        , _sdc_table_version
        , _sdc_sequence
        , _sdc_received_at
        , _sdc_batched_at
        
    from {{ source('adventureworks_etl', 'specialoffer') }}
)
select * from source