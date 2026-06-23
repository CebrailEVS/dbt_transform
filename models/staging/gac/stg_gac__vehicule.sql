{{
  config(
    materialized='table',
    description='Parc véhicule EVS (GAC → SFTP SG), nettoyé et restreint aux colonnes utiles. Grain source 1:1 (1 ligne = 1 période de contrat type / déclaration AEN)',
  )
}}


with source as (
    select * from {{ source('gac', 'gac_parc_vehicule') }}
),

cleaned as (
    select
        -- Contrat
        nullif(trim(contrat_id_gac), '') as contrat_id_gac,
        nullif(trim(contrat_immatriculation_edi), '') as contrat_immatriculation_edi,
        nullif(trim(contrat_statut_actuel), '') as contrat_statut_actuel,
        nullif(trim(contrat_motif_de_cl_ture), '') as contrat_motif_de_cloture,

        -- Ligne de contrat type (déclaration AEN)
        nullif(trim(contrat_type_etat), '') as contrat_type_etat,

        -- Disponibilité (dernier événement)
        nullif(trim(dernier_v_nement_disponibilit_du_v_hicule), '') as dernier_evenement_disponibilite_du_vehicule,

        -- Collaborateur
        nullif(trim(collaborateur_fonction_actuelle), '') as collaborateur_fonction_actuelle,

        -- Dates
        safe.parse_timestamp('%d/%m/%Y %H:%M:%S', nullif(trim(date_de_saisie), '')) as date_de_saisie,
        safe.parse_date('%d/%m/%Y', nullif(trim(contrat_type_date_d_effet), '')) as contrat_type_date_d_effet,
        safe.parse_date('%d/%m/%Y', nullif(trim(contrat_type_date_de_fin), '')) as contrat_type_date_de_fin,
        safe.parse_date('%d/%m/%Y', nullif(trim(dernier_v_nement_date_d_effet), '')) as dernier_evenement_date_d_effet,
        safe.parse_date('%d/%m/%Y', nullif(trim(dernier_v_nement_date_de_fin), '')) as dernier_evenement_date_de_fin,

        -- Métadonnées Meltano
        _sdc_extracted_at as extracted_at,
        _sdc_deleted_at as deleted_at

    from source
)

select * from cleaned
