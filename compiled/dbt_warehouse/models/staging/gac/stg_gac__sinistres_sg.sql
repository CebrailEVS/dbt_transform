-- TEST SELECTIVE RUN, test de fdp



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
        cast(co_t_assureur as float64) as cout_assureur,
        cast(auto_assurance as float64) as auto_assurance,
        cast(franchise as float64) as franchise,
        cast(co_t_global as float64) as cout_global,
        cast(co_t_client as float64) as cout_client,

        -- Date
        safe.parse_timestamp('%d/%m/%Y %H:%M:%S', nullif(trim(date), '')) as date_sinistre,
        safe.parse_timestamp('%d/%m/%Y %H:%M:%S', nullif(trim(date_de_cr_ation), '')) as date_de_creation,

        -- Date annexe
        safe.parse_date('%d/%m/%Y', nullif(trim(date_de_passage_de_l_expert), '')) as date_passage_expert,
        safe.parse_date('%d/%m/%Y', nullif(trim(date_de_r_ception_du_constat), '')) as date_reception_constat,
        safe.parse_date(
            '%d/%m/%Y', nullif(trim(date_d_envoi_du_constat_l_assureur), '')
        ) as date_envoie_constat_assureur,
        safe.parse_date(
            '%d/%m/%Y', nullif(trim(date_de_d_but_de_r_paration), '')
        ) as date_debut_reparation,
        safe.parse_date(
            '%d/%m/%Y', nullif(trim(date_de_fin_de_r_paration), '')
        ) as date_fin_reparation,
        safe.parse_date(
            '%d/%m/%Y',
            nullif(trim(date_de_remise_du_v_hicule_au_collaborateur_apr_s_r_paration), '')
        ) as date_remise_vehicule_collaborateur,
        safe.parse_date(
            '%d/%m/%Y', nullif(trim(date_de_cl_ture_du_sinistre), '')
        ) as date_cloture_sinistre,

        -- Métadonnées Meltano
        _sdc_source_file,
        _sdc_source_lineno,
        _sdc_received_at,
        _sdc_batched_at,
        _sdc_sequence

    from source
)

select * from cleaned