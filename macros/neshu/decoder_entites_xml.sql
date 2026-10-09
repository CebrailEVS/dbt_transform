{#
    Décode les entités XML d'une chaîne extraite par regexp.

    Nécessaire car une expression régulière ne décode pas les entités XML
    (contrairement à un parseur XML) : sans ce décodage, `&apos;` resterait littéral.

    `&amp;` est traité EN DERNIER, sinon `&amp;apos;` deviendrait `'` au lieu de
    `&apos;` — l'esperluette doit être la dernière entité restaurée.
#}
{% macro decoder_entites_xml(colonne) %}
    replace(replace(replace(replace(replace(
        {{ colonne }},
        '&apos;', "'"), '&quot;', '"'), '&lt;', '<'), '&gt;', '>'), '&amp;', '&')
{% endmacro %}
