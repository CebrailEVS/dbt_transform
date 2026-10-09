{{
    config(
        materialized = 'table',
        description = 'Stocks théoriques Yuman normalisés depuis le fichier SFTP du fournisseur'
    )
}}

-- PAS DE DÉDUPLICATION : le pipeline dlt écrit en merge/delete-insert sur export_date, une
-- journée rejouée écrase la précédente au chargement, le doublon ne peut pas atteindre dbt.

with source as (

    select *
    from {{ source('yuman_evs_sftp', 'sftp_yuman_evs_stock_theorique') }}

),

cleaned as (

    select
        -- Les noms bruts sont ceux que le normaliseur dlt produit à partir des en-têtes accentués
        -- du fichier (`Référence`, `Désignation`…). `quantitx` n'est pas une coquille : c'est la
        -- mutilation de l'accent final par dlt.
        trim(r_f_rence) as reference,
        trim(d_signation) as designation,

        -- Le fournisseur écrit la virgule décimale. Le cast reste ici : la
        -- couche d'ingestion laisse le raw fidèle à la source.
        cast(replace(quantitx, ',', '.') as float64) as quantite,

        -- Chaîne vide quand l'emplacement n'est pas renseigné : le fournisseur écrit
        -- `réf;désignation;qté;"";""`.
        nullif(trim(nom_du_stock), '') as nom_du_stock,

        export_date,

        -- CLÉ DE LIGNE. Le fichier n'a pas de clé naturelle : des lignes partagent
        -- (export_date, référence, nom_du_stock), ce sont de vrais doublons de la source, conservés
        -- tels quels. `_dlt_id` est l'identifiant de ligne généré par dlt. Même usage que dans
        -- stg_zoho_desk__ticket_history, qui n'a pas non plus d'`id` source.
        _dlt_id

    from source

)

select *
from cleaned
