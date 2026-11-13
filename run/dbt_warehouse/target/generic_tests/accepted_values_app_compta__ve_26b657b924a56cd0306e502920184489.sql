
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        disponibilite as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_app_compta`.`app_compta__vehicule_imputation`
    group by disponibilite

)

select *
from all_values
where value_field not in (
    'Affecté','Disponible','En maintenance','Indisponible'
)



  
  
      
    ) dbt_internal_test