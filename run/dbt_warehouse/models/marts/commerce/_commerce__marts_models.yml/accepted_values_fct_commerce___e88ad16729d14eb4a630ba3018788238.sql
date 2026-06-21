
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        opp_statut as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_marts`.`fct_commerce__opportunite`
    group by opp_statut

)

select *
from all_values
where value_field not in (
    'Gagné','Perdu','En cours'
)



  
  
      
    ) dbt_internal_test