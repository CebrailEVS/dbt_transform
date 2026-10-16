{#
    Heure de Paris à afficher, rangée dans un TIMESTAMP : contrat de sortie de la
    couche gold (marts, apps) pour les instants venus d'un ERP Oracle.

    Power BI et l'app Cockpit affichent la valeur d'un TIMESTAMP telle quelle,
    comme une heure murale. La couche gold leur rend donc l'heure de Paris, à
    partir de l'instant UTC de la couche silver : ni le type ni les valeurs ne
    changent pour eux. Ce n'est PAS un instant : ne pas le comparer à
    current_timestamp() ni à une autre source, ne pas le réinjecter en silver.

        {{ utc_vers_paris_affichage('task_start_date') }} as task_start_date
#}
{% macro utc_vers_paris_affichage(colonne) -%}
    timestamp(datetime({{ colonne }}, 'Europe/Paris'))
{%- endmacro %}
