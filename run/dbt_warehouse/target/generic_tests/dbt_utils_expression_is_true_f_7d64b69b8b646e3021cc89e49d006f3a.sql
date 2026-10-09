
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_bi__activite_rapport_jour`

where not(nb_consultations > 0 or nb_rafraichissements > 0)


  
  
      
    ) dbt_internal_test