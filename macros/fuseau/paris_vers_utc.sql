{#
    Instant en vrai UTC à partir d'une heure de Paris stockée sans fuseau.

    Les ERP Oracle Distrilog (Neshu, LCDP, Nayka) écrivent l'heure de Paris sans
    fuseau, et dlt la charge en TIMESTAMP comme si c'était de l'UTC. Le staging
    déclare le fuseau d'origine : la couche silver est en vrai UTC.

    Réservée aux instants. Une date calendaire (toujours à minuit) se type en
    DATE avec date(colonne) : convertie en instant, elle reculerait à la veille.

        {{ paris_vers_utc('modification_date') }} as updated_at
#}
{% macro paris_vers_utc(colonne) -%}
    timestamp(datetime({{ colonne }}), 'Europe/Paris')
{%- endmacro %}
