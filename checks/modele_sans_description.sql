{{ config(severity='warn') }}
-- Règle : tout modèle a une description YAML, la seule publiée dans BigQuery en v2.
-- En warn tant que le stock existe ; passer en error une fois résorbé.
select original_file_path as modele
from {{ info_schema('models') }}
where original_file_path like 'models/%'
  and coalesce(trim(description), '') = ''
