
    
    

with child as (
    select idpayment_condition_facredev as from_field
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__company`
    where idpayment_condition_facredev is not null
),

parent as (
    select idpayment_condition as to_field
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__payment_condition`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


