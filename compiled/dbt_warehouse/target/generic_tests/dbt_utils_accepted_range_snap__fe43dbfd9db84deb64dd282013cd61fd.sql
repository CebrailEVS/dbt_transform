

with meet_condition as(
  select *
  from `evs-datastack-prod`.`snapshots`.`snap_oracle_neshu__valo_parc_machines`
),

validation_errors as (
  select *
  from meet_condition
  where
    -- never true, defaults to an empty result set. Exists to ensure any combo of the `or` clauses below succeeds
    1 = 2
    -- records with a value >= min_value are permitted. The `not` flips this to find records that don't meet the rule.
    or not nombre_machines >= 0
)

select *
from validation_errors

