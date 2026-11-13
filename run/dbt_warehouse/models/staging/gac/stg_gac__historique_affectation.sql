
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_gac__historique_affectation`
      
    
    

    
    OPTIONS(
      description="""Historique des affectations des v\u00e9hicules EVS (fichier GAC quotidien, prod_raw.gac_historique_affectation), nettoy\u00e9 et restreint aux colonnes du contrat, de l'\u00e9v\u00e8nement et du collaborateur. Grain source 1:1 : 1 ligne = 1 \u00e9v\u00e8nement d'un contrat dans 1 fichier dat\u00e9 ; un \u00e9v\u00e8nement revient dans chaque fichier tant qu'il est dans la fen\u00eatre de 12 mois de GAC. La derni\u00e8re version de chaque \u00e9v\u00e8nement se reconstitue dans int_gac__affectation_vehicule.\n"""
    )
    as (
      


with source_data as (
    select * from `evs-datastack-prod`.`prod_raw`.`gac_historique_affectation`
),

cleaned_data as (
    select
        -- Journée du fichier : chaque dépôt quotidien s'empile, la fenêtre de GAC glisse
        snapshot_date,

        -- Contrat
        nullif(trim(contrat_id_gac), '') as contrat_id_gac,
        nullif(trim(contrat_immatriculation_edi), '') as contrat_immatriculation_edi,
        nullif(trim(contrat_statut_actuel), '') as contrat_statut_actuel,
        safe.parse_date('%d/%m/%Y', nullif(trim(contrat_date_de_cloture), '')) as contrat_date_de_cloture,
        nullif(trim(contrat_motif_de_cloture), '') as contrat_motif_de_cloture,
        nullif(trim(type_de_contrat), '') as type_de_contrat,
        nullif(trim(fournisseur), '') as fournisseur,
        nullif(trim(vehicule_genre_fiscal), '') as vehicule_genre_fiscal,

        -- Évènement : une période de disponibilité du véhicule
        safe.parse_date('%d/%m/%Y', nullif(trim(date_d_effet), '')) as date_d_effet,
        safe.parse_date('%d/%m/%Y', nullif(trim(date_de_fin), '')) as date_de_fin,
        nullif(trim(disponibilite_du_vehicule), '') as disponibilite_du_vehicule,
        nullif(trim(commentaire_de_l_evenement), '') as commentaire_de_l_evenement,
        safe.parse_date(
            '%d/%m/%Y', nullif(trim(date_de_modification_de_l_evenement), '')
        ) as date_de_modification_de_l_evenement,

        -- Collaborateur affecté sur la période
        nullif(trim(field_colleague_id), '') as field_colleague_id,
        nullif(trim(collaborateur), '') as collaborateur,
        nullif(trim(nom), '') as nom,
        nullif(trim(prenom), '') as prenom,
        nullif(trim(email), '') as email,
        nullif(trim(fonction_actuelle), '') as fonction_actuelle,

        timestamp(_extracted_at) as extracted_at

    from source_data
)

select * from cleaned_data
    );
  