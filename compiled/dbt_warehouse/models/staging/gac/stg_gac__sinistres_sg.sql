-- TEST SELECTIVE RUN, num2



with source as (
    select * from `evs-datastack-prod`.`prod_raw`.`gac_suivi_sinistres_sg`
),

cleaned as (
    select
        -- Nettoyage des chaînes vides en NULL
        nullif(trim(n_de_sinistre), '') as n_de_sinistre,
        nullif(trim(type), '') as type_sinistre,
        nullif(trim(r_f_rence_gac), '') as reference_gac,
        nullif(trim(immat_), '') as immat,
        nullif(trim(circonstance), '') as circonstance,
        nullif(trim(cl_tur_), '') as cloture,
        nullif(trim(description), '') as description,
        nullif(trim(tiers), '') as tiers,
        nullif(trim(resp_), '') as resp,
        nullif(trim(matricule), '') as matricule,
        nullif(trim(nom), '') as nom,
        nullif(trim(pr_nom), '') as prenom,
        nullif(trim(centre_de_co_ts), '') as centre_de_couts,
        nullif(trim(genre_fiscal), '') as genre_fiscal,
        nullif(trim(statut_actuel), '') as statut_actuel,
        nullif(trim(remboursement), '') as remboursement,
        nullif(trim(constat_envoy_l_assureur), '') as constat_envoye_assureur,
        nullif(trim(franchise_sup_rieure_la_r_paration), '') as franchise_superieure_reparation,
        nullif(trim(trajet_domicile_travail), '') as trajet_domicile_travail,
        nullif(trim(accident_de_week_end), '') as accident_de_week_end,
        nullif(trim(lieu_du_sinistre), '') as lieu_du_sinistre,
        nullif(trim(entit_entit_1), '') as entite_entite_1,
        nullif(trim(entit_entit_2), '') as entite_entite_2,
        nullif(trim(entit_entit_3), '') as entite_entite_3,
        nullif(trim(entit_entit_4), '') as entite_entite_4,

        -- Mesure
        co_t_assureur as cout_assureur,
        auto_assurance,
        franchise,
        co_t_global as cout_global,
        co_t_client as cout_client,

        -- Date
        SAFE.PARSE_TIMESTAMP('%d/%m/%Y %H:%M:%S', NULLIF(TRIM(date), '')) AS date_sinistre,
        SAFE.PARSE_TIMESTAMP('%d/%m/%Y %H:%M:%S', NULLIF(TRIM(date_de_cr_ation), '')) AS date_de_creation,

        -- Date annexe
        SAFE.PARSE_DATE('%d/%m/%Y', NULLIF(TRIM(date_de_passage_de_l_expert), '')) AS date_passage_expert,
        SAFE.PARSE_DATE('%d/%m/%Y', NULLIF(TRIM(date_de_r_ception_du_constat), '')) AS date_reception_constat,
        SAFE.PARSE_DATE('%d/%m/%Y', NULLIF(TRIM(date_d_envoi_du_constat_l_assureur), '')) AS date_envoie_constat_assureur,
        SAFE.PARSE_DATE('%d/%m/%Y', NULLIF(TRIM(date_de_d_but_de_r_paration), '')) AS date_debut_reparation,
        SAFE.PARSE_DATE('%d/%m/%Y', NULLIF(TRIM(date_de_fin_de_r_paration), '')) AS date_fin_reparation,
        SAFE.PARSE_DATE('%d/%m/%Y', NULLIF(TRIM(date_de_remise_du_v_hicule_au_collaborateur_apr_s_r_paration), '')) AS date_remise_vehicule_collaborateur,
        SAFE.PARSE_DATE('%d/%m/%Y', NULLIF(TRIM(date_de_cl_ture_du_sinistre), '')) AS date_cloture_sinistre,

        -- Métadonnées Meltano
        _sdc_source_file,
        _sdc_source_lineno,
        _sdc_received_at,
        _sdc_batched_at,
        _sdc_sequence
        
    from source
)

select * from cleaned