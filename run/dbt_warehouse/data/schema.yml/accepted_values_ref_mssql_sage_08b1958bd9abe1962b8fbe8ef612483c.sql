
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        budg_categorie_pnl as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_reference`.`ref_mssql_sage__pnl_budget`
    group by budg_categorie_pnl

)

select *
from all_values
where value_field not in (
    'CA','MASSE_SALARIALE','CONSOMMATION_MP_SSTT'
)



  
  
      
    ) dbt_internal_test