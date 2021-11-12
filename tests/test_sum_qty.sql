with
    validation as (
        select sum(orderqty) as sum_val
        from {{ref ('fact_salesorder') }}
        where orderdate_fk between '2011-05-31' and '2014-06-30'
    )
select * from validation where sum_val != 274914