-- Règle : un intermediate ne lit que le staging de sa source (docs/conventions/intermediate.md) ;
-- le croisement de sources se fait dans les marts. Une variante de dépôt (`oracle_neshu_gcs`)
-- compte comme sa source (`oracle_neshu`).
select c.name as intermediate, p.name as staging_lu
from {{ info_schema('edges') }} as e
inner join {{ info_schema('models') }} as c on c.unique_id = e.child_unique_id
inner join {{ info_schema('models') }} as p on p.unique_id = e.parent_unique_id
where c.original_file_path like 'models/intermediate/%'
  and p.original_file_path like 'models/staging/%'
  and not starts_with(
      split_part(p.original_file_path, '/', 3),
      split_part(c.original_file_path, '/', 3)
  )
