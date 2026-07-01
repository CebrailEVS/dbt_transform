{# Source unique de vérité : un emplacement Yuman (nom_du_stock) est un dépôt si son
   libellé contient 'DEPOT' (4 sites), sinon un stock technicien (van). #}

{% macro yuman_is_depot(col) -%}
({{ col }} like '%DEPOT%')
{%- endmacro %}

{% macro yuman_stock_type(col) -%}
case when {{ yuman_is_depot(col) }} then 'DEPOT' else 'TECHNICIEN' end
{%- endmacro %}
