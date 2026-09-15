{#
    Extrait la valeur d'une balise d'une colonne XML stockée en texte.

    L'ERP Distrilog range des attributs libres dans une colonne XMLTYPE, que dlt
    charge en texte. BigQuery n'a pas de fonctions XML : on lit la balise par
    expression régulière.

    Le patron existait déjà, inliné dans stg_oracle_neshu__contract ; il est
    factorisé ici parce que trois modèles l'utilisent désormais
    (/ZONE/COUTRM sur task, /ZONE/EFFECTIF sur company, /ZONE/CLOC sur product).

    Ne décode PAS les entités XML : enchaîner avec decoder_entites_xml() quand la
    valeur attendue est du texte libre. Inutile pour une valeur numérique.

        {{ extraire_balise_xml('xml', 'COUTRM') }}
#}
{% macro extraire_balise_xml(colonne, balise) -%}
    regexp_extract({{ colonne }}, r'<{{ balise }}>([^<]*)</{{ balise }}>')
{%- endmacro %}
