{#
    Ventes Nunshen (Sage, domaine 0 : BL type 3, factures types 6 et 7, avoirs compris), mêmes règles dans tous les
    modèles de l'application : une vente intragroupe est une pièce de la souche « ZZ » ; un BL encore sans facture
    (échantillons, gratuits) est une pièce de type 3. Faux hors ventes.
#}
{% macro nunshen_is_intragroupe(do_domaine, do_type, do_piece) -%}
coalesce({{ do_domaine }} = 0 and {{ do_type }} in (3, 6, 7) and starts_with({{ do_piece }}, 'ZZ'), false)
{%- endmacro %}

{% macro nunshen_is_non_facture(do_domaine, do_type) -%}
coalesce({{ do_domaine }} = 0 and {{ do_type }} = 3, false)
{%- endmacro %}
