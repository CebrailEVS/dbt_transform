{#
    Extrait la valeur d'une balise d'une colonne XML stockée en texte.

    L'ERP Distrilog range des attributs libres dans une colonne XMLTYPE, que dlt
    charge en texte. BigQuery n'a pas de fonctions XML : on lit la balise par
    expression régulière.

    Utilisée par int_oracle_neshu__charges_sociales et int_oracle_neshu__telemetrie_parc.
    stg_oracle_neshu__contract a son propre regexp_extract.

    Ne décode PAS les entités XML : enchaîner avec decoder_entites_xml() quand la
    valeur attendue est du texte libre. Inutile pour une valeur numérique.

        {{ extraire_balise_xml('xml', 'COUTRM') }}
#}
{% macro extraire_balise_xml(colonne, balise) -%}
    regexp_extract({{ colonne }}, r'<{{ balise }}>([^<]*)</{{ balise }}>')
{%- endmacro %}
