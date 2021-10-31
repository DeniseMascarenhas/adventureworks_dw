with source as (
    select
        /* Primary Key */
        businessentityid
        
        /* Entity attributes */
        , nationalidnumber
        , loginid
        , jobtitle
        , birthdate
        , maritalstatus
        , gender
        , hiredate
        , salariedflag
        , vacationhours
        , sickleavehours
        , currentflag
        , organizationnode
        , rowguid
        , modifieddate
        
        /* Stitch columns */
        , _sdc_table_version
        , _sdc_sequence
        , _sdc_received_at
        , _sdc_batched_at
        
    from {{ source('adventureworks_etl', 'employee') }}
)
select * from source