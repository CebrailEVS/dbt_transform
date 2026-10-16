





with validation_errors as (

    select
        idresources, idlabel_family
    from (select * from `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__v_label_resources` where lf_exclus = 1) dbt_subquery
    group by idresources, idlabel_family
    having count(*) > 1

)

select *
from validation_errors


