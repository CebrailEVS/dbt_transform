
    
    



select is_active
from (select * from `evs-datastack-prod`.`prod_staging`.`stg_yuman__users` where user_id != 13857) dbt_subquery
where is_active is null


