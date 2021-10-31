with source as (
    select
        /* Primary Key */
        productcategoryid
        , productsubcategoryid
    
        /* Entity attributes */
        , name
        , rowguid
        , modifieddate
    
        /* Stitch columns */
        , _sdc_table_version
        , _sdc_sequence
        , _sdc_received_at
        , _sdc_batched_at
        
    from {{ source('adventureworks_etl', 'productsubcategory') }}
)
select * from source