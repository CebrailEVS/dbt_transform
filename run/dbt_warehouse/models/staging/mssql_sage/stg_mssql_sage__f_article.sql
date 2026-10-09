
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_article`
      
    
    

    
    OPTIONS(
      description="""R\u00e9f\u00e9rentiel articles Nunshen \u2014 dimension des lignes de documents, du stock et des nomenclatures."""
    )
    as (
      

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`dbo_f_article`
),

cleaned_data as (
    select
        -- Identifiant technique Sage (PK)
        cb_marq,

        -- Clé métier : unique dans Sage (contrainte UKA_F_ARTICLE_AR_Ref)
        ar_ref,
        -- U+0090 = « É » saisi en CP850 (MASTER SELECTION LIMITÉE)
        nullif(trim(replace(ar_design, '\u0090', 'É')), '') as ar_design,
        nullif(trim(ar_raccourci), '') as ar_raccourci,
        nullif(trim(ar_substitut), '') as ar_substitut,
        nullif(trim(ar_langue1), '') as ar_langue1,
        nullif(trim(ar_langue2), '') as ar_langue2,
        nullif(trim(ar_photo), '') as ar_photo,

        -- Classement
        nullif(trim(fa_code_famille), '') as fa_code_famille,
        ar_type,
        ar_nature,
        nullif(trim(ar_stat01), '') as ar_stat01,
        nullif(trim(ar_stat02), '') as ar_stat02,
        nullif(trim(ar_stat03), '') as ar_stat03,
        nullif(trim(ar_stat04), '') as ar_stat04,
        nullif(trim(ar_stat05), '') as ar_stat05,
        cl_no1,
        cl_no2,
        cl_no3,
        cl_no4,
        co_no,

        -- Codification
        nullif(trim(ar_code_barre), '') as ar_code_barre,
        nullif(trim(ar_edi_code), '') as ar_edi_code,
        nullif(trim(ar_code_fiscal), '') as ar_code_fiscal,
        nullif(trim(ar_pays), '') as ar_pays,

        -- Valorisation et tarifs
        ar_prix_ach,
        ar_coef,
        ar_prix_ven,
        ar_prix_ttc,
        ar_pu_net,
        ar_cout_std,
        ar_prix_ach_nouv,
        ar_coef_nouv,
        ar_prix_ven_nouv,
        nullif(ar_date_application, timestamp('1753-01-01')) as ar_date_application,
        ar_escompte,
        ar_vte_debit,

        -- Unités et poids
        ar_unite_ven,
        ar_unite_poids,
        ar_poids_net,
        ar_poids_brut,
        ar_nb_colis,
        ar_condition,
        ar_fact_poids,
        ar_fact_forfait,
        ar_saisie_var,

        -- Gestion de stock et fabrication
        ar_suivi_stock,
        ar_nomencl,
        ar_gamme1,
        ar_gamme2,
        ar_qte_comp,
        ar_qte_operatoire,
        ar_prevision,
        nullif(trim(rp_code_defaut), '') as rp_code_defaut,
        ar_delai,
        ar_delai_fabrication,
        ar_delai_peremption,
        ar_delai_securite,
        ar_type_lancement,
        ar_cycle,
        ar_criticite,
        ar_fictif,
        ar_sous_traitance,
        ar_contremarque,
        ar_garantie,

        -- Statut
        ar_sommeil,
        ar_publie,
        ar_hors_stat,
        ar_not_imp,
        ar_transfere,
        ar_interdire_commande,
        ar_exclure,
        nullif(ar_date_modif, timestamp('1753-01-01')) as ar_date_modif,

        -- Frais d'approche (3 frais × 3 composantes)
        nullif(trim(ar_frais01_fr_denomination), '') as ar_frais01_fr_denomination,
        ar_frais01_fr_rem01_rem_valeur,
        ar_frais01_fr_rem01_rem_type,
        ar_frais01_fr_rem02_rem_valeur,
        ar_frais01_fr_rem02_rem_type,
        ar_frais01_fr_rem03_rem_valeur,
        ar_frais01_fr_rem03_rem_type,
        nullif(trim(ar_frais02_fr_denomination), '') as ar_frais02_fr_denomination,
        ar_frais02_fr_rem01_rem_valeur,
        ar_frais02_fr_rem01_rem_type,
        ar_frais02_fr_rem02_rem_valeur,
        ar_frais02_fr_rem02_rem_type,
        ar_frais02_fr_rem03_rem_valeur,
        ar_frais02_fr_rem03_rem_type,
        nullif(trim(ar_frais03_fr_denomination), '') as ar_frais03_fr_denomination,
        ar_frais03_fr_rem01_rem_valeur,
        ar_frais03_fr_rem01_rem_type,
        ar_frais03_fr_rem02_rem_valeur,
        ar_frais03_fr_rem02_rem_type,
        ar_frais03_fr_rem03_rem_valeur,
        ar_frais03_fr_rem03_rem_type,

        -- Champs libres Nunshen (saisie manuelle, OUI/NON en texte : typage en aval)
        nullif(trim(certification_bio), '') as certification_bio,
        nullif(trim(autre_certification), '') as autre_certification,
        nullif(trim(fabrication_interne), '') as fabrication_interne,
        nullif(trim(source_en_direct), '') as source_en_direct,
        nullif(trim(signature_exclusivite_client), '') as signature_exclusivite_client,
        nullif(trim(exclusivite_boutique), '') as exclusivite_boutique,
        nullif(trim(produit_sur_demande), '') as produit_sur_demande,
        nullif(trim(prime), '') as prime,
        nullif(trim(allergene), '') as allergene,
        nullif(trim(provenance), '') as provenance,
        nullif(trim(blend_name_2), '') as blend_name_2,
        -- U+0090 = « É » saisi en CP850 (MASTER SELECTION LIMITÉE)
        nullif(trim(replace(selection_3, '\u0090', 'É')), '') as selection_3,
        -- U+0090 = « É » saisi en CP850 (MASTER SELECTION LIMITÉE)
        nullif(trim(replace(vente_par_selection, '\u0090', 'É')), '') as vente_par_selection,
        nullif(trim(type_de_the_4), '') as type_de_the_4,
        nullif(trim(type_de_the_5), '') as type_de_the_5,
        nullif(trim(densite_du_the), '') as densite_du_the,
        nullif(trim(note_olfactive), '') as note_olfactive,
        nullif(trim(ambiance), '') as ambiance,
        nullif(trim(grs_tasse), '') as grs_tasse,
        nullif(trim(prix_ttc_tasse), '') as prix_ttc_tasse,
        nullif(trim(dimensions_du_produit), '') as dimensions_du_produit,
        nullif(trim(bin_code), '') as bin_code,
        nullif(trim(emplacement_entrepot), '') as emplacement_entrepot,

        -- Metadata
        cb_creation as created_at,
        coalesce(cb_modification, cb_creation) as updated_at,
        _extracted_at as extracted_at

    from source_data
)

select *
from cleaned_data
-- Défensif : le raw est un snapshot complet, donc ar_ref y est déjà unique.
qualify row_number() over (
    partition by ar_ref
    order by updated_at desc, cb_marq desc
) = 1
    );
  