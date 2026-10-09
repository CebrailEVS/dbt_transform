
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        delivery_status_code as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__commande_fournisseur_tasks`
    group by delivery_status_code

)

select *
from all_values
where value_field not in (
    'LIVRE','LIVRE_PARTIEL','EN_ATTENTE'
)



  
  
      
    ) dbt_internal_test