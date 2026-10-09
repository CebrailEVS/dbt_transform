
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        classe_demande as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__classification_article_neshu`
    group by classe_demande

)

select *
from all_values
where value_field not in (
    'regulier','erratique','intermittent','lumpy','indetermine'
)



  
  
      
    ) dbt_internal_test