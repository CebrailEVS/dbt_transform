-- N° d'intervention Nomad hors 7 chiffres : Cockpit Supply rapproche une pose d'astreinte de sa régularisation
-- Yuman (« ASTREINTE <n°> ») sur ce n°. Rattaché aussi au dernier modèle de la chaîne : en échec, le job échoue
-- (alerte) sans qu'aucune table saute, dbt ne bloquant que les modèles en aval de tous les parents du test.
-- depends_on: {{ ref('app_cockpit__technique_conso_nespresso_mensuelle') }}
select
    num_intervention,
    count(*) as nb_lignes
from {{ ref('app_cockpit__technique_conso_nespresso_detail') }}
where not regexp_contains(num_intervention, r'^[0-9]{7}$')
group by num_intervention
