{{
    config(
        materialized = 'table',
        description = 'Stocks théoriques Yuman normalisés depuis le fichier SFTP du fournisseur'
    )
}}

-- Remplace stg_yuman_gcs__stock_theorique, à sortie IDENTIQUE sur les colonnes
-- métier. Seules la source et deux détails techniques changent.
--
-- PAS DE DÉDUPLICATION, contrairement au modèle qu'il remplace. Celui-ci
-- dédupliquait sur (export_date, _sdc_source_file, _sdc_source_lineno) pour
-- absorber le 16/02/2026, journée déposée DEUX fois dans GCS. Le pipeline dlt
-- écrit désormais en merge/delete-insert sur export_date : une journée rejouée
-- écrase la précédente au chargement, le doublon ne peut plus atteindre dbt.
-- Vérifié ligne à ligne le 2026-08-02 : la table raw rend exactement les mêmes
-- 1 992 233 lignes que l'ancien modèle, doublons de contenu compris.

with source as (

    select *
    from {{ source('yuman_evs_sftp', 'sftp_yuman_evs_stock_theorique') }}

),

cleaned as (

    select
        -- Les noms bruts sont ceux que le normaliseur dlt produit à partir des
        -- en-têtes accentués du fichier (`Référence`, `Désignation`…).
        -- `quantitx` n'est pas une coquille : dlt et Meltano ne mutilent pas
        -- l'accent final de la même façon — Meltano donnait `quantit_`.
        trim(r_f_rence) as reference,
        trim(d_signation) as designation,

        -- Le fournisseur écrit la virgule décimale. Le cast reste ici : la
        -- couche d'ingestion laisse le raw fidèle à la source.
        cast(replace(quantitx, ',', '.') as float64) as quantite,

        -- Chaîne vide quand l'emplacement n'est pas renseigné — 3 044 lignes
        -- par jour, que le fournisseur écrit `réf;désignation;qté;"";""`.
        nullif(trim(nom_du_stock), '') as nom_du_stock,

        export_date,

        -- CLÉ DE LIGNE. Le fichier n'a pas de clé naturelle : 1 463 lignes sur
        -- 2 002 398 partagent (export_date, référence, nom_du_stock), et ce
        -- sont de vrais doublons de la source, conservés tels quels. `_dlt_id`
        -- est l'identifiant de ligne généré par dlt ; il tient le rôle que
        -- `_sdc_source_lineno` tenait sous Meltano. Même usage que dans
        -- stg_zoho_desk__ticket_history, qui n'a pas non plus d'`id` source.
        _dlt_id

    from source

)

select *
from cleaned
