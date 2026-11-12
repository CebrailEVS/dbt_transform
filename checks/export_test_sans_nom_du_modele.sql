-- Règle : un test d'un export porte le nom du modèle (docs/conventions/exports.md § 6). Le job
-- n'envoie le fichier que si les tests de sa table ont réussi, et les reconnaît à ce nom dans les
-- résultats de dbt-runner : un test singulier nommé autrement serait ignoré sans bruit.
select m.name as export, t.name as test
from {{ info_schema('edges') }} as e
inner join {{ info_schema('models') }} as m on m.unique_id = e.parent_unique_id
inner join {{ info_schema('data_tests') }} as t on t.unique_id = e.child_unique_id
where m.original_file_path like 'models/exports/%'
  and strpos(t.name, m.name) = 0
