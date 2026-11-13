
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        disponibilite_du_vehicule as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_staging`.`stg_gac__historique_affectation`
    group by disponibilite_du_vehicule

)

select *
from all_values
where value_field not in (
    'Affecté','Disponible','En maintenance','Indisponible'
)



  
  
      
    ) dbt_internal_test