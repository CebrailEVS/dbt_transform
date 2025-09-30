

with meet_condition as(
  select *
  from `evs-datastack-prod`.`prod_marts`.`fct_oracle_neshu__conso_business_review`
),

validation_errors as (
  select *
  from meet_condition
  where
    -- never true, defaults to an empty result set. Exists to ensure any combo of the `or` clauses below succeeds
    1 = 2
    -- records with a value >= min_value are permitted. The `not` flips this to find records that don't meet the rule.
    or not consumption_date >= '2020-01-01'
    -- records with a value <= max_value are permitted. The `not` flips this to find records that don't meet the rule.
    or not consumption_date <= current_date()
)

select *
from validation_errors

