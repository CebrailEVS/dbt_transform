{#
    Règles de l'application Cockpit Supply reprises en SQL pour sa couche de service.
    Source de vérité côté app : data/csv_source.py, get_rupture_depot_yuman_historique
    (_fournisseur / _normalize_ref) — préfixes sensibles à la casse, remplacements de
    préfixe (et non retrait du seul préfixe de tête), code en majuscules.
#}

{% macro cockpit_fournisseur_rupture_tech(ref) -%}
case
    when starts_with({{ ref }}, 'EVS_NESPRESSO_') or starts_with({{ ref }}, 'EVS_NESP_') then 'Nespresso'
    when starts_with({{ ref }}, 'EVS_NESTLE_') then 'Nestlé'
    when starts_with({{ ref }}, 'ANIM_') then 'Animo'
    when starts_with({{ ref }}, 'EVS_BRITA_') then 'Brita'
    when starts_with({{ ref }}, 'BRIT') then 'Brita_GF'
    when starts_with({{ ref }}, 'TWYD') or starts_with({{ ref }}, 'TYWD') then 'TYWD'
    when starts_with({{ ref }}, 'AUUM_') then 'AUUM'
end
{%- endmacro %}

{% macro cockpit_code_article_rupture_tech(ref, fournisseur) -%}
upper(case
    when {{ fournisseur }} = 'Nespresso' then replace(replace({{ ref }}, 'EVS_NESPRESSO_', ''), 'EVS_NESP_', '')
    when {{ fournisseur }} = 'Nestlé' then replace({{ ref }}, 'EVS_NESTLE_', '')
    else {{ ref }}
end)
{%- endmacro %}
