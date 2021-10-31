with source as (
    select
        /* Primary Key */
        productid
        
        /* Entity attributes */
        , name
        , productsubcategoryid
        , productnumber
        , sellenddate
        , safetystocklevel
        , finishedgoodsflag
        , class
        , makeflag
        , reorderpoint
        , productmodelid
        , weightunitmeasurecode
        , standardcost
        , style
        , sizeunitmeasurecode
        , listprice
        , daystomanufacture
        , productline
        , size
        , color
        , sellstartdate
        , weight
        , rowguid
        , modifieddate

        /* Stitch columns */
        , _sdc_table_version
        , _sdc_sequence
        , _sdc_received_at
        , _sdc_batched_at
                
    from {{ source('adventureworks_etl', 'product') }}
)
select * from source