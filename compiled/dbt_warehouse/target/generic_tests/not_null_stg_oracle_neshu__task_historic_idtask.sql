
    
    



select idtask
from (select * from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task_historic` where idhistoric_action not in (129, 130, 131)) dbt_subquery
where idtask is null


