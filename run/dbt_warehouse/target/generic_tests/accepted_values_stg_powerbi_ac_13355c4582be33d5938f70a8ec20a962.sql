
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        content_provider_type as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_staging`.`stg_powerbi_activity__datasets`
    group by content_provider_type

)

select *
from all_values
where value_field not in (
    'PbixInImportMode','UsageMetricsUserReport','PbixInCompositeMode','InImportMode','Unknown'
)



  
  
      
    ) dbt_internal_test