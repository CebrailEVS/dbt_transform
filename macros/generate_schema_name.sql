{#
    Routage des datasets par environnement.

    prod : comportement dbt par défaut, <target.schema>_<+schema>
           → prod_staging, prod_intermediate, prod_marts, prod_reference.
    autre (dev, ci) : TOUT dans le seul dataset du target (dbt_<dev>,
           dbt_ci_pr_<N>). Les préfixes stg_/int_/dim_/fct_/ref_ rendent les
           noms uniques entre couches ; les ref() non construits partent en
           --defer vers prod_*.

    Garde-fou : hors prod, un dataset prod_* est refusé à la compilation. L'IAM
    le refuse déjà (aucune identité non-prod n'écrit dans prod_*) ; ceci fait
    échouer plus tôt, avec un message lisible.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if target.name == 'prod' -%}
        {%- if custom_schema_name is none -%}
            {{ target.schema }}
        {%- else -%}
            {{ target.schema }}_{{ custom_schema_name | trim }}
        {%- endif -%}
    {%- else -%}
        {%- if target.schema.startswith('prod') -%}
            {{ exceptions.raise_compiler_error(
                "Target '" ~ target.name ~ "' pointe vers le dataset '" ~ target.schema
                ~ "' : hors prod, un dataset prod_* est interdit. Vérifier DBT_BIGQUERY_DATASET_DEV."
            ) }}
        {%- endif -%}
        {{ target.schema }}
    {%- endif -%}
{%- endmacro %}
