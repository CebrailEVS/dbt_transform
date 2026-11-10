-- Le commentaire Distrilog d'une livraison interne est un texte libre (noms de clients) : seul le n° de
-- facture e-shop en est extrait. Toute colonne de commentaire dans la table de l'app fait échouer le test.
{%- set relation = ref('app_cockpit__lcdp_livraison_interne') %}
select column_name
from `{{ relation.database }}`.`{{ relation.schema }}`.INFORMATION_SCHEMA.COLUMNS  -- noqa: CP02
where
    table_name = '{{ relation.identifier }}'
    and regexp_contains(lower(column_name), r'comment')
