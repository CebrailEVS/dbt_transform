
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        destination_kind as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__lcdp_mouvement_machine`
    group by destination_kind

)

select *
from all_values
where value_field not in (
    'DEPOT','CLIENT','FOURNISSEUR','SOCIETE','EQUIPEMENT','AUTRE','AUCUNE'
)



  
  
      
    ) dbt_internal_test