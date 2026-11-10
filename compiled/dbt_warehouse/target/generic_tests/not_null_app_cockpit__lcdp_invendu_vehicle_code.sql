
    
    



select vehicle_code
from (select * from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__lcdp_invendu` where task_start_date >= timestamp_sub(current_timestamp(), interval 35 day)) dbt_subquery
where vehicle_code is null


