

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`dbo_f_docentete`
),

cleaned_data as (
    select
        -- Identifiant technique Sage (PK)
        cb_marq,

        -- Identification du document (clé métier : do_type, do_piece)
        do_domaine,
        do_type,
        nullif(trim(do_piece), '') as do_piece,
        do_doc_type,
        do_date,
        nullif(trim(do_heure), '') as do_heure,
        do_statut,
        do_valide,
        do_provenance,
        nullif(trim(do_ref), '') as do_ref,
        nullif(trim(do_piece_orig), '') as do_piece_orig,
        nullif(trim(do_ref_externe), '') as do_ref_externe,
        nullif(trim(do_no_web), '') as do_no_web,
        nullif(trim(do_guid), '') as do_guid,
        do_imprim,
        do_attente,
        do_conversion,
        do_exclure,

        -- Tiers et interlocuteurs
        nullif(trim(do_tiers), '') as do_tiers,
        nullif(trim(ct_num_payeur), '') as ct_num_payeur,
        nullif(trim(ct_num_centrale), '') as ct_num_centrale,
        co_no,
        li_no,
        et_no,
        nullif(trim(do_contact), '') as do_contact,
        nullif(trim(do_coord01), '') as do_coord01,
        nullif(trim(do_coord02), '') as do_coord02,
        nullif(trim(do_coord03), '') as do_coord03,
        nullif(trim(do_coord04), '') as do_coord04,

        -- Logistique et livraison
        de_no,
        nullif(do_date_livr, timestamp('1753-01-01')) as do_date_livr,
        nullif(do_date_livr_realisee, timestamp('1753-01-01')) as do_date_livr_realisee,
        nullif(do_date_expedition, timestamp('1753-01-01')) as do_date_expedition,
        do_expedit,
        do_condition,
        do_colisage,
        do_type_colis,
        do_transaction,
        do_regime,
        mr_no,
        nullif(trim(do_code_service), '') as do_code_service,

        -- Comptabilité, analytique, devise
        nullif(trim(cg_num), '') as cg_num,
        nullif(trim(ca_num), '') as ca_num,
        nullif(trim(ca_num_ifrs), '') as ca_num_ifrs,
        n_cat_compta,
        do_period,
        do_devise,
        do_cours,
        do_tarif,
        do_langue,
        do_souche,
        do_ecart,
        do_ventile,
        do_maj_cpta,
        do_transfere,
        do_cloture,
        do_nb_facture,
        do_bl_fact,
        do_reliquat,
        do_tva_debit,
        do_type_calcul,
        do_clause_reserve,
        do_escompte,
        do_tx_escompte,

        -- Abonnement
        ab_no,
        nullif(do_debut_abo, timestamp('1753-01-01')) as do_debut_abo,
        nullif(do_fin_abo, timestamp('1753-01-01')) as do_fin_abo,
        nullif(do_debut_period, timestamp('1753-01-01')) as do_debut_period,
        nullif(do_fin_period, timestamp('1753-01-01')) as do_fin_period,

        -- Caisse
        ca_no,
        co_no_caissier,

        -- Frais de port et franco
        do_type_frais,
        do_val_frais,
        do_type_ligne_frais,
        do_type_franco,
        do_val_franco,
        do_type_ligne_franco,

        -- Taxes
        do_taxe1,
        do_type_taux1,
        do_type_taxe1,
        nullif(trim(do_code_taxe1), '') as do_code_taxe1,
        do_taxe2,
        do_type_taux2,
        do_type_taxe2,
        nullif(trim(do_code_taxe2), '') as do_code_taxe2,
        do_taxe3,
        do_type_taux3,
        do_type_taxe3,
        nullif(trim(do_code_taxe3), '') as do_code_taxe3,

        -- Totaux
        do_total_ht,
        do_total_ht_net,
        do_total_ttc,
        do_net_a_payer,

        -- Règlement et facturation électronique
        do_montant_regle,
        nullif(trim(do_ref_paiement), '') as do_ref_paiement,
        nullif(trim(do_adresse_paiement), '') as do_adresse_paiement,
        do_paiement_ligne,
        do_statut_bap,
        do_facture_elec,
        nullif(trim(do_facture_frs), '') as do_facture_frs,
        nullif(trim(do_facture_file), '') as do_facture_file,
        do_type_transac,
        do_e_statut,
        do_demande_regul,
        do_coffre,
        nullif(trim(do_motif), '') as do_motif,
        do_motif_devis,
        eb_no,
        cfar_no,
        fac_no,

        -- Champs libres Nunshen
        nullif(trim(ref_chronopost), '') as ref_chronopost,
        nullif(trim(poids), '') as poids,
        nullif(trim(canal_de_reception), '') as canal_de_reception,
        nullif(trim(temp), '') as temp,  -- noqa: RF04
        nullif(trim(b2_b_commentaire_client), '') as b2_b_commentaire_client,
        nullif(trim(n_de_bl), '') as n_de_bl,
        nullif(trim(coi), '') as coi,
        nullif(trim(cmr_waybill), '') as cmr_waybill,

        -- Metadata
        cb_creation as created_at,
        coalesce(cb_modification, cb_creation) as updated_at,
        _extracted_at as extracted_at

    from source_data
)

select *
from cleaned_data