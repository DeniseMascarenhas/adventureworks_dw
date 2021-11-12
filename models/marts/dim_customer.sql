with
    person as (
        select *
        from {{ ref('stg_person') }}
    )
    , stage_selected as (
        select
            customer.customerid
            , concat(IFNULL(firstname, ''), ' ', IFNULL(middlename, ''), ' ', IFNULL(lastname, '')) as customername
            , customer.personid     
            
        from {{ ref('stg_customer') }} as customer
        left join person on customer.personid = person.businessentityid
),
transformed as (
    select
        row_number() over (order by customerid) as customer_sk
        , *
    from stage_selected
)
select * from transformed