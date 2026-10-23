
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        objet_intervent as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__inter_technique_tasks`
    group by objet_intervent

)

select *
from all_values
where value_field not in (
    'OC01','OC02','OC03','OC04','OC05','OC06','OC07','OC08'
)



  
  
      
    ) dbt_internal_test