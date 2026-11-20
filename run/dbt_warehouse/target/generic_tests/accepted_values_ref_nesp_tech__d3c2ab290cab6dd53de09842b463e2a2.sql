
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        famille as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_reference`.`ref_nesp_tech__piece_controle_stock`
    group by famille

)

select *
from all_values
where value_field not in (
    'kit','hors_kit'
)



  
  
      
    ) dbt_internal_test