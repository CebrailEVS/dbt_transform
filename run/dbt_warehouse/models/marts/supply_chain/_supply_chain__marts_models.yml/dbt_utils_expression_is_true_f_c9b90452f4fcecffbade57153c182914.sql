
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__rupture_depot_yuman`

where not(is_out_of_stock_global = (qty_depot + qty_autres_depots + qty_vans_total = 0))


  
  
      
    ) dbt_internal_test