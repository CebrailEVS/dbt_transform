
    
    



select dbt_invocation_id
from `evs-datastack-prod`.`prod_marts`.`fct_oracle_neshu__conso_business_review`
where dbt_invocation_id is null


