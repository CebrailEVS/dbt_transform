{% set dbt_custom_arg_expression -%}
stock_at_date = coalesce(stock_inventaire, 0) + coalesce(plus, 0) - coalesce(moins, 0)

{%- endset %}

{{ dbt_utils.test_expression_is_true(expression=dbt_custom_arg_expression, model=get_where_subquery(ref('fct_supply_chain__stock_lcdp'))) }}