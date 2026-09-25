
    
    

with all_values as (

    select
        appel_categorie_code as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_marts`.`fct_lcdp__appel_sav`
    group by appel_categorie_code

)

select *
from all_values
where value_field not in (
    'APP01','APP02','APP03','APP04','APP05','APP06','APP07','APP08','APP09'
)


