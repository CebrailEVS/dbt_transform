-- Règle : la description d'un mart suit les 4 blocs (docs/conventions/marts.md § 2) :
-- c'est elle que lisent Power BI, le DA et les agents.
select name as mart
from {{ info_schema('models') }}
where original_file_path like 'models/marts/%'
  and not (
      coalesce(description, '') like '%[QUOI MÉTIER]%'
      and description like '%[COMMENT CONSTRUITE]%'
      and description like '%[GRAIN]%'
      and description like '%[NOTES]%'
  )
