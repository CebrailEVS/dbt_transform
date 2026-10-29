
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  

with all_values as (

    select distinct
        activity as value_field

    from `evs-datastack-prod`.`prod_staging`.`stg_powerbi_activity__events`
    
    where created_at >= timestamp_sub(current_timestamp(), interval 3 day)
    

),
set_values as (

    select
        'ViewReport' as value_field
    union all
    select
        'RefreshDataset' as value_field
    
    

),
unique_set_values as (

    select distinct value_field
    from
        set_values

),
validation_errors as (
    -- values in set that are not in the list of values from the model
    select
        s.value_field
    from
        unique_set_values s
        left join
        all_values v on s.value_field = v.value_field
    where
        v.value_field is null

)

select *
from validation_errors


  
  
      
    ) dbt_internal_test