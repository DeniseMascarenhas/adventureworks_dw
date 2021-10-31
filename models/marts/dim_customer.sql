with
    person as (
        select *
        from {{ ref('stg_person') }}
    )
    , stage_selected as (
        select
            /* customer */
            customerid
            , territoryid
            , storeid
            /* person */
            , firstname
            , middlename
            , lastname
            
        from {{ ref('stg_customer') }} as customer
        left join person on customer.customerid = person.businessentityid
),
transformed as (
    select
        row_number() over (order by customerid) as customer_sk
        , *
    from stage_selected
)
select * from transformed