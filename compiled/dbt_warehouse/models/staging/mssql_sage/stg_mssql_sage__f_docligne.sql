

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`dbo_f_docligne`
),

cleaned_data as (
    select
        -- Identifiant technique Sage (PK)
        cb_marq,
        cb_co_no as cbco_no, -- FK pour table collaborateur (nom historique conservé)

        -- Identifiant de la ligne (clé métier) et historique de numérotation
        dl_no,
        dl_ligne,
        nullif(trim(old_dl_no), '') as old_dl_no,

        -- Document : domaine et type Sage (DO_Domaine : 0 = ventes, 1 = achats, 2 = stock)
        do_domaine,
        do_type,
        do_doc_type,
        nullif(trim(do_piece), '') as do_piece,
        do_date,
        nullif(trim(do_ref), '') as do_ref,
        nullif(trim(ct_num), '') as ct_num,
        co_no,

        -- Documents d'origine recopiés à la transformation (BC = commande, BL = livraison, PL = préparation, DE = devis)
        nullif(trim(dl_piece_bc), '') as dl_piece_bc,
        nullif(dl_date_bc, timestamp('1753-01-01')) as dl_date_bc,
        dl_qte_bc,
        nullif(trim(dl_piece_bl), '') as dl_piece_bl,
        nullif(dl_date_bl, timestamp('1753-01-01')) as dl_date_bl,
        dl_qte_bl,
        nullif(trim(dl_piece_pl), '') as dl_piece_pl,
        nullif(dl_date_pl, timestamp('1753-01-01')) as dl_date_pl,
        dl_qte_pl,
        dl_type_pl,
        nullif(trim(dl_piece_de), '') as dl_piece_de,
        nullif(dl_date_de, timestamp('1753-01-01')) as dl_date_de,
        dl_qte_de,
        nullif(do_date_livr, timestamp('1753-01-01')) as do_date_livr,
        dl_piece_of_prod,

        -- Article
        nullif(trim(ar_ref), '') as ar_ref,
        nullif(trim(dl_design), '') as dl_design,
        nullif(trim(ar_ref_compose), '') as ar_ref_compose,
        dl_t_nomencl,
        ag_no1,
        ag_no2,
        nullif(trim(af_ref_fourniss), '') as af_ref_fourniss,
        nullif(trim(ac_ref_client), '') as ac_ref_client,
        nullif(trim(ancien_code_article), '') as ancien_code_article,
        nullif(trim(eu_enumere), '') as eu_enumere,
        eu_qte,
        dl_poids_net,
        dl_poids_brut,

        -- Stock
        de_no,
        dl_mvt_stock,
        dl_cmup,
        dl_prix_ru,
        dl_valorise,
        dl_non_livre,
        nullif(trim(dl_no_colis), '') as dl_no_colis,
        dl_no_ref,
        dl_no_link,
        dl_no_sous_total,

        -- Quantité, prix, remises
        dl_qte,
        dl_prix_unitaire,
        dl_pubc,
        dl_pu_devise,
        dl_puttc,
        dl_ttc,
        dl_remise01_rem_valeur,
        dl_remise01_rem_type,
        dl_remise02_rem_valeur,
        dl_remise02_rem_type,
        dl_remise03_rem_valeur,
        dl_remise03_rem_type,
        dl_t_rem_pied,
        dl_t_rem_exep,
        dl_escompte,
        dl_frais,
        dl_fact_poids,

        -- Montants
        dl_montant_ht,
        dl_montant_ttc,

        -- Taxes
        dl_taxe1,
        dl_type_taux1,
        dl_type_taxe1,
        nullif(trim(dl_code_taxe1), '') as dl_code_taxe1,
        dl_taxe2,
        dl_type_taux2,
        dl_type_taxe2,
        nullif(trim(dl_code_taxe2), '') as dl_code_taxe2,
        dl_taxe3,
        dl_type_taux3,
        dl_type_taxe3,
        nullif(trim(dl_code_taxe3), '') as dl_code_taxe3,

        -- Analytique, production, divers
        nullif(trim(ca_num), '') as ca_num,
        ca_no,
        dt_no,
        nullif(trim(rp_code), '') as rp_code,
        dl_qte_ressource,
        nullif(dl_date_avancement, timestamp('1753-01-01')) as dl_date_avancement,
        nullif(trim(pf_num), '') as pf_num,
        nullif(trim(dl_operation), '') as dl_operation,
        nullif(trim(dl_ref_externe), '') as dl_ref_externe,

        -- Metadata
        cb_creation as created_at,
        coalesce(cb_modification, cb_creation) as updated_at,
        _extracted_at as extracted_at
    from source_data
)

select *
from cleaned_data
qualify row_number() over (
    partition by dl_no
    order by updated_at desc, cb_marq desc
) = 1