
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        kpi as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_marts`.`fct_mssql_sage__pnl_bu_kpis`
    group by kpi

)

select *
from all_values
where value_field not in (
    'CA','CONSOMMATION_MP_SSTT','MASSE_SALARIALE','FRAIS_DIRECTS_AMORTISSEMENTS','MARGE_BRUTE','MARGE_NETTE'
)



  
  
      
    ) dbt_internal_test