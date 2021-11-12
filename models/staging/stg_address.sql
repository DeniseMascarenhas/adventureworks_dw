with
    countryregion as (
        select *
        from {{ source('adventureworks_etl', 'countryregion') }}
    )
    , stateprovince as (
        select *
        from {{ source('adventureworks_etl', 'stateprovince') }}
    )
    , source_selected as (
        select
            /* Primary Key */
            addressid
        
            /* Entity attributes */
            , addressline1
            , addressline2
            , city
            , stateprovince.stateprovinceid
            , stateprovince.name as stateprovincename
            , stateprovince.countryregioncode
            , countryregion.name as countryregionname
            , postalcode
            , spatiallocation
            , address.rowguid
            , address.modifieddate

            /* Stitch columns */
            , address._sdc_table_version
            , address._sdc_sequence
            , address._sdc_received_at
            , address._sdc_batched_at
        
        from {{ source('adventureworks_etl', 'address') }} as address
        left join stateprovince on address.stateprovinceid = stateprovince.stateprovinceid
        left join countryregion on stateprovince.countryregioncode = countryregion.countryregioncode
)
select * from source_selected