-- Règle : un fait porte un test d'unicité sur son grain (docs/conventions/marts.md § 7) :
-- sans lui, un doublon passe en silence jusqu'aux totaux de Power BI.
select m.name as fait
from {{ info_schema('models') }} as m
where m.original_file_path like 'models/marts/%'
  and m.name like 'fct\_%' escape '\'
  and not exists (
      select 1
      from {{ info_schema('data_tests') }} as t
      where t.node_unique_id = m.unique_id
        and t.test_name like '%unique%'
  )
