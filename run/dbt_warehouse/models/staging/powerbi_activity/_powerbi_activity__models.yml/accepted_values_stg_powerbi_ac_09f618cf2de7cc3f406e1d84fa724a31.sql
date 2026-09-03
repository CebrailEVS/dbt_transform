
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        activity as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_staging`.`stg_powerbi_activity__events`
    group by activity

)

select *
from all_values
where value_field not in (
    'ViewReport','RefreshDataset','GenerateScreenshot','GetSnapshots','ExportReport','UpdateApp','InstallApp','GetCloudSupportedDatasources','SetScheduledRefresh','ExportArtifact','ExportArtifactDownload'
)



  
  
      
    ) dbt_internal_test