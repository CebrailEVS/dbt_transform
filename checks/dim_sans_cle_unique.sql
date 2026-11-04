-- Règle : une dimension porte un test d'unicité sur sa clé (docs/conventions/marts.md § 7) :
-- une clé en double démultiplie chaque fait qui la joint.
select m.name as dimension
from {{ info_schema('models') }} as m
where m.original_file_path like 'models/marts/%'
  and m.name like 'dim\_%' escape '\'
  and not exists (
      select 1
      from {{ info_schema('data_tests') }} as t
      where t.node_unique_id = m.unique_id
        and t.test_name like '%unique%'
  )
