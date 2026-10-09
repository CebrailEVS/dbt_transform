
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        contrat_statut_actuel as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_staging`.`stg_gac__vehicule`
    group by contrat_statut_actuel

)

select *
from all_values
where value_field not in (
    'A la route','Clos','En attente de livraison'
)



  
  
      
    ) dbt_internal_test