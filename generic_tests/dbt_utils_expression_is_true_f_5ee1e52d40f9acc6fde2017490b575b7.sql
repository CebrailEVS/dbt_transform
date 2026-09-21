{% set dbt_custom_arg_expression -%}
not (
  tech_id_reel_astreinte is not null
  and tech_id_reel_modif is not null
  and tech_id_reel_astreinte != tech_id_reel_modif
)

{%- endset %}

{{ dbt_utils.test_expression_is_true(expression=dbt_custom_arg_expression, model=get_where_subquery(ref('fct_technique__intervention_retraitee'))) }}{{ config({"meta":{},"severity":"ERROR","tags":[]}) }}