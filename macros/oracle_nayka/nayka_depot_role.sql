{#
    Rôle d'un dépôt Nayka dans le stock, d'après son code société Distrilog (règle validée avec le
    Supply) : les agences portent le stock ; MARKETING (cadeaux clients, showroom) et DEPOTREBUT en
    sont exclus. Une seule définition pour les équipements et pour le stock de produits.
#}
{% macro nayka_depot_role(code) -%}
case
    when {{ code }} = 'DEPOTREBUT' then 'rebut'
    when {{ code }} = 'MARKETING' then 'hors_stock'
    else 'stock'
end
{%- endmacro %}
