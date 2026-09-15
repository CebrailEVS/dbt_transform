

-- Table de traduction de l'ERP : un idstring, une ligne par langue. C'est elle
-- qui donne son libellé lisible à un label (`label.idstring`), là où
-- `label.code` ne porte que le code technique — `1J2` contre `1 jour sur 2`.
--
-- Les deux colonnes forment la clé : filtrer sur langage_code = 'fr_FR' est le
-- travail de l'aval, pas du staging.

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`evs_string`
),

cleaned_data as (
    select
        -- IDs convertis en BIGINT
        cast(idstring as int64) as idstring,

        -- Colonnes texte
        langage_code,
        text,
        context,
        var,
        comments,

        -- Booléen
        cast(cast(ihm as int64) as boolean) as is_ihm,

        -- Timestamps harmonisés
        timestamp(modification_date) as updated_at,
        timestamp(_extracted_at) as extracted_at

    from source_data
)

select * from cleaned_data