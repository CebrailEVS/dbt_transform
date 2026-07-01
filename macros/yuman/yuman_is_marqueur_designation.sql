{# Vrai si la désignation Yuman est un "marqueur" (note de gestion) et non un vrai libellé
   d'article : "- NE PAS UTILISER -", "OLD - ...", "Remplacé par 1055767", etc.
   Sert à écarter ces libellés quand une même référence porte plusieurs désignations
   (dédup de designation dans fct_supply_chain__stock_article_yuman).
   Regex volontairement étroite (validée sur données) pour NE PAS attraper de vraies pièces
   ("VANNE D'ARRET", "Kit remplacement …", "… HOLDER"). #}

{% macro yuman_is_marqueur_designation(col) -%}
regexp_contains(upper({{ col }}), r'NE PAS UTILISER|^OLD |REMPLAC.{0,4}PAR')
{%- endmacro %}
