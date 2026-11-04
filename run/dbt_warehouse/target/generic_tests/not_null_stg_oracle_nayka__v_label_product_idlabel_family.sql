
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select idlabel_family
from `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__v_label_product`
where idlabel_family is null



  
  
      
    ) dbt_internal_test