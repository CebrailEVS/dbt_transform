
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select nom_departement
from `evs-datastack-prod`.`prod_reference`.`ref_yuman__cp_tech`
where nom_departement is null



  
  
      
    ) dbt_internal_test