-- Règle : un export part du staging, l'intermediate par exception, jamais d'un mart ni d'une app
-- (docs/conventions/exports.md) : le fichier d'un destinataire ne suit pas l'évolution des rapports.
select c.name as export, p.name as modele_lu
from {{ info_schema('edges') }} as e
inner join {{ info_schema('models') }} as c on c.unique_id = e.child_unique_id
inner join {{ info_schema('models') }} as p on p.unique_id = e.parent_unique_id
where c.original_file_path like 'models/exports/%'
  and (p.original_file_path like 'models/marts/%' or p.original_file_path like 'models/apps/%')
