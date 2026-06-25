
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        budg_bu as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_reference`.`ref_mssql_sage__pnl_budget`
    group by budg_bu

)

select *
from all_values
where value_field not in (
    'NESHU','NUNSHEN','COMMERCE','TECHNIQUE'
)



  
  
      
    ) dbt_internal_test