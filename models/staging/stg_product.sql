with
    productsubcategory as (
        select *
        from {{ source('adventureworks_etl', 'productsubcategory') }}
    )
    , productcategory as (
        select *
        from {{ source('adventureworks_etl', 'productcategory') }}
    )
    , source_selected as (
        select
            /* Primary Key */
            productid
        
            /* Entity attributes */
            , product.name as productname
            , productsubcategory.productsubcategoryid
            , productsubcategory.name as productsubcategoryname
            , productcategory.productcategoryid
            , productcategory.name as productcategoryname
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
            , product.rowguid
            , product.modifieddate

            /* Stitch columns */
            , product._sdc_table_version
            , product._sdc_sequence
            , product._sdc_received_at
            , product._sdc_batched_at

        from {{ source('adventureworks_etl', 'product') }} as product
        left join productsubcategory on product.productsubcategoryid = productsubcategory.productsubcategoryid
        left join productcategory on productsubcategory.productcategoryid = productcategory.productcategoryid
)
select * from source_selected