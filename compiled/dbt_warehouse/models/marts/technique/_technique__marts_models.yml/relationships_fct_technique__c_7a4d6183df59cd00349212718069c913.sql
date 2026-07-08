
    
    

with child as (
    select technician_id as from_field
    from `evs-datastack-prod`.`prod_marts`.`fct_technique__consommation_article_nespresso`
    where technician_id is not null
),

parent as (
    select user_id as to_field
    from `evs-datastack-prod`.`prod_marts`.`dim_technique__technician`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


