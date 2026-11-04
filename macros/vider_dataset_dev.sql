{#
    Vide ton dataset de dev : à lancer au début de chaque branche, avant le premier build.

    --defer lit ta table dev dès qu'elle existe, même ancienne, à la place de la
    prod. Partir d'un dataset vide garantit que tout ce que tu ne construis pas
    est lu en prod, à jour.

    Sans argument, liste ce qui serait supprimé, avec la date de création, les plus récents
    d'abord : un objet récent qui ne vient pas de ta branche signale un autre chantier en cours.
    Ne supprime qu'avec confirmer: true.
        dbt run-operation vider_dataset_dev
        dbt run-operation vider_dataset_dev --args '{confirmer: true}'

    Vide les objets, jamais le dataset : il appartient à Terraform (expiration,
    droits) et dbt-dev ne peut pas le recréer. Refuse tout target autre que dev
    et tout dataset hors dbt_<nom> (ni dbt_ci_pr_<N>, ni prod_*).
#}
{% macro vider_dataset_dev(confirmer=false) %}
    {%- if target.name != 'dev' -%}
        {{ exceptions.raise_compiler_error(
            "vider_dataset_dev : target '" ~ target.name ~ "' refusé, réservé au target dev."
        ) }}
    {%- endif -%}

    {%- set dataset = target.schema -%}
    {%- if not dataset.startswith('dbt_') or dataset.startswith('dbt_ci') -%}
        {{ exceptions.raise_compiler_error(
            "vider_dataset_dev : dataset '" ~ dataset ~ "' refusé, seul un dataset personnel dbt_<nom>"
            ~ " se vide. Vérifier DBT_BIGQUERY_DATASET_DEV."
        ) }}
    {%- endif -%}

    {#- Chaque type BigQuery a son DROP ; BASE TABLE et CLONE (dbt clone) prennent drop table. -#}
    {%- set drop_par_type = {
        'VIEW': 'drop view if exists',
        'MATERIALIZED VIEW': 'drop materialized view if exists',
        'EXTERNAL': 'drop external table if exists',
        'SNAPSHOT': 'drop snapshot table if exists'
    } -%}
    {%- set chemin = target.database ~ '.' ~ dataset -%}
    {%- set objets = run_query(
        "select table_name, table_type,"
        ~ " format_timestamp('%Y-%m-%d %H:%M', creation_time, 'Europe/Paris') as cree_le"
        ~ " from `" ~ chemin ~ ".INFORMATION_SCHEMA.TABLES` order by creation_time desc"
    ) -%}

    {%- if objets.rows | length == 0 -%}
        {{ log(dataset ~ " est déjà vide.", info=true) }}
    {%- else -%}
        {%- set ordres = [] -%}
        {%- for objet in objets.rows -%}
            {%- do ordres.append(
                drop_par_type.get(objet['table_type'], 'drop table if exists')
                ~ " `" ~ chemin ~ "." ~ objet['table_name'] ~ "`"
            ) -%}
        {%- endfor -%}

        {%- if confirmer == true -%}
            {%- do run_query(ordres | join(";\n")) -%}
            {{ log(dataset ~ " vidé : " ~ ordres | length ~ " objets supprimés.", info=true) }}
        {%- else -%}
            {%- for objet in objets.rows -%}
                {{ log(
                    "  " ~ objet['cree_le'] ~ "  " ~ objet['table_name'] ~ "  (" ~ objet['table_type'] ~ ")",
                    info=true
                ) }}
            {%- endfor -%}
            {{ log(
                ordres | length ~ " objets seraient supprimés de " ~ dataset ~ ". Pour confirmer : "
                ~ "dbt run-operation vider_dataset_dev --args '{confirmer: true}'",
                info=true
            ) }}
        {%- endif -%}
    {%- endif -%}
{% endmacro %}
