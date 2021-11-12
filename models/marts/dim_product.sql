with
    stage_selected as (
        select
            productid
            , productname
            , productsubcategoryid
            , productsubcategoryname
            , productcategoryid
            , productcategoryname
            , makeflag
            , finishedgoodsflag
            , safetystocklevel
            , reorderpoint
            , standardcost
            , listprice
            , color
            , size
            , sizeunitmeasurecode
            , weightunitmeasurecode
            , weight 
            , daystomanufacture
            , productline
            , class  
            , style
            , sellstartdate
            , sellenddate         

        from {{ ref('stg_product') }} as product
),
transformed as (
    select
        row_number() over (order by productid) as product_sk
        , *
    from stage_selected
)
select * from transformed