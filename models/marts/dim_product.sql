with
    productsubcategory as (
        select *
        from {{ ref('stg_productsubcategory') }}
    )
    , productcategory as (
        select *
        from {{ ref('stg_productcategory') }}
    )
    , stage_selected as (
        select
        /* employee */
        productid
        , product.name as product_name
        , productsubcategory.productsubcategoryid
        , productsubcategory.name as productsubcategory_name
        , productcategory.productcategoryid
        , productcategory.name as productcategory_name
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

        from {{ ref('stg_product') }} as product
        left join productsubcategory on product.productsubcategoryid = productsubcategory.productsubcategoryid
        left join productcategory on productsubcategory.productcategoryid = productcategory.productcategoryid
),
transformed as (
    select
        row_number() over (order by productid) as product_sk
        , *
    from stage_selected
)
select * from transformed