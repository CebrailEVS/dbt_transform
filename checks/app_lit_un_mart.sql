-- Règle : un modèle apps part du staging ou de l'intermediate, jamais d'un mart
-- (docs/conventions/apps.md) : un mart change au rythme des rapports, pas de l'application.
select c.name as app, p.name as mart_lu
from {{ info_schema('edges') }} as e
inner join {{ info_schema('models') }} as c on c.unique_id = e.child_unique_id
inner join {{ info_schema('models') }} as p on p.unique_id = e.parent_unique_id
where c.original_file_path like 'models/apps/%'
  and p.original_file_path like 'models/marts/%'
