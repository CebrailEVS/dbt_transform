{% set dbt_custom_arg_expression -%}
(src_inter = 'NESP' and agency is not null) or (src_inter = 'YUMAN' and agency is null)

{%- endset %}

{{ dbt_utils.test_expression_is_true(expression=dbt_custom_arg_expression, model=get_where_subquery(ref('fct_technique__intervention'))) }}