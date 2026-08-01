{#
    Décode les entités XML d'une chaîne extraite par regexp.

    Nécessaire depuis que le parsing du XML de CONTRACT est fait en SQL plutôt que
    par un script Python : ElementTree décodait les entités, une expression
    régulière non. Sur les 306 contrats de NESHU, un seul contient `&apos;`, et
    sans ce décodage la valeur différait de celle produite par l'ancien script.

    `&amp;` est traité EN DERNIER, sinon `&amp;apos;` deviendrait `'` au lieu de
    `&apos;` — l'esperluette doit être la dernière entité restaurée.
#}
{% macro decoder_entites_xml(colonne) %}
    replace(replace(replace(replace(replace(
        {{ colonne }},
        '&apos;', "'"), '&quot;', '"'), '&lt;', '<'), '&gt;', '>'), '&amp;', '&')
{% endmacro %}
