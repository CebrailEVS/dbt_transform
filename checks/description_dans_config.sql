-- Règle : la description d'un modèle va dans le YAML, jamais dans le config() (CONVENTIONS.md) :
-- seule celle du YAML est publiée, et deux copies finissent par diverger. dbt range une
-- description de config() parmi les clés qu'il ne connaît pas (__warehouse_specific_config__).
select name as modele
from {{ info_schema('models') }}
where original_file_path like 'models/%'
  and json_extract_string(
      json_extract_string(config, '$')::json,
      '$.__warehouse_specific_config__.description'
  ) is not null
