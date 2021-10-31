with
    person as (
        select *
        from {{ ref('stg_person') }}
    )
    , employee as (
        select *
        from {{ ref('stg_employee') }}
    )
    , stage_selected as (
        select
            salesperson.businessentityid
            /* person */
            , firstname
            , middlename
            , lastname
            /* salesperson */
            , territoryid
            , salesquota
            , bonus           
            , commissionpct   
            , salesytd  
            , saleslastyear
            /* employee */
            , jobtitle
            , birthdate
            , maritalstatus
            , gender
            , hiredate

        from {{ ref('stg_salesperson') }} as salesperson
        left join employee on salesperson.businessentityid = employee.businessentityid
        left join person on employee.businessentityid = person.businessentityid
),
transformed as (
    select
        row_number() over (order by businessentityid) as salesperson_sk
        , *
    from stage_selected
)
select * from transformed