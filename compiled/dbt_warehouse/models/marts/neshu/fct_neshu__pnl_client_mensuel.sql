

-- Compte de résultat mensuel par client Neshu, reprise du rapport produit en
-- interne par les développeurs de Distrilog.
--
-- Chaînage : CA - coûts produits = marge brute, puis - amortissement - coût de
-- main-d'oeuvre - télémétrie = marge nette.
--
-- Les écarts connus avec le rapport Distrilog sont documentés dans le YAML de ce
-- modèle, section [NOTES]. Les lire AVANT de conclure à une erreur de calcul.

with charges_du_mois as (

    -- Enveloppe mensuelle unique, à répartir.
    select
        mois,
        charges_sociales_eur
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__charges_sociales`
),

temps_total_du_mois as (

    -- Dénominateur de la répartition. Il couvre TOUS les clients ayant reçu un
    -- passage, y compris ceux qui n'apparaîtront pas dans ce mart faute de CA :
    -- c'est la règle de Distrilog, et elle fait que la somme des coûts affichés
    -- est légèrement inférieure aux charges réelles (cf. [NOTES]).
    select
        mois,
        sum(nb_min_rm) as nb_min_total
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__temps_appro_mensuel`
    group by mois
),

amortissement_client as (

    select
        mois,
        company_id,
        round(sum(montant_amortissement_eur), 2) as montant_amortissement_eur
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__amortissement_machines`
    group by mois, company_id
),

assemble as (

    select
        ca.mois,
        ca.company_id,
        ca.company_code,

        -- Chiffre d'affaires
        ca.ca_vending_ht_eur,
        ca.ca_presta_service_ht_eur,
        ca.ca_fontaines_ht_eur,
        ca.ca_laves_verres_ht_eur,
        ca.ca_nayax_ht_eur,
        ca.ca_negoce_ht_eur,
        ca.ca_total_ht_eur,

        -- Coûts produits. Le coût du paiement sans contact est un pourcentage du
        -- CA télémétrie, paramétré par var('taux_cout_cashless').
        coalesce(cp.cout_produits_charges_ht_eur, 0) as cout_produits_charges_ht_eur,
        coalesce(cp.cout_produits_livres_ht_eur, 0) as cout_produits_livres_ht_eur,
        round(ca.ca_nayax_ht_eur * 0.02, 2) as cout_cashless_ht_eur,

        -- Exploitation
        coalesce(ta.nb_min_rm, 0) as nb_min_rm,
        coalesce(ta.nb_passages_machine, 0) as nb_passages_machine,
        coalesce(ta.nb_passages_client, 0) as nb_passages_client,
        coalesce(am.montant_amortissement_eur, 0) as montant_amortissement_eur,
        coalesce(tp.cout_telemetrie_eur, 0) as cout_telemetrie_eur,

        -- Répartition du coût de main-d'oeuvre au prorata du temps
        round(
            safe_divide(coalesce(ta.nb_min_rm, 0), tt.nb_min_total)
            * coalesce(cm.charges_sociales_eur, 0), 2
        ) as cout_rm_eur,
        round(safe_divide(cm.charges_sociales_eur, tt.nb_min_total), 2) as cout_minute_eur

    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__ca_client_mensuel` as ca
    left join `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__cout_produits_mensuel` as cp
        on ca.mois = cp.mois and ca.company_id = cp.company_id
    left join `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__temps_appro_mensuel` as ta
        on ca.mois = ta.mois and ca.company_id = ta.company_id
    left join amortissement_client as am
        on ca.mois = am.mois and ca.company_id = am.company_id
    left join `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__telemetrie_parc` as tp
        on ca.mois = tp.mois and ca.company_id = tp.company_id
    left join charges_du_mois as cm on ca.mois = cm.mois
    left join temps_total_du_mois as tt on ca.mois = tt.mois
),

avec_marges as (

    select
        a.*,
        round(
            a.cout_produits_charges_ht_eur + a.cout_produits_livres_ht_eur
            + a.cout_cashless_ht_eur, 2
        ) as cout_produits_total_ht_eur,
        round(
            a.ca_total_ht_eur - a.cout_produits_charges_ht_eur
            - a.cout_produits_livres_ht_eur - a.cout_cashless_ht_eur, 2
        ) as marge_brute_eur,
        round(
            a.ca_total_ht_eur - a.cout_produits_charges_ht_eur
            - a.cout_produits_livres_ht_eur - a.cout_cashless_ht_eur
            - a.montant_amortissement_eur - a.cout_rm_eur - a.cout_telemetrie_eur, 2
        ) as marge_nette_eur
    from assemble as a
)

select
    -- 🔑 Grain
    m.mois,
    m.company_id,

    -- 📇 Signalétique client, portée par la dimension — y compris le code et le
    -- nom. Les lire dans le fait laissait sans nom les clients qui n'ont que du
    -- CA télémétrie un mois donné, faute de ligne de facturation d'où le tirer.
    d.company_code,
    d.company_name,
    d.postal_code,
    d.city,
    cc.first_contract_date,
    -- Typologie : client acquis dans l'année DU MOIS ANALYSÉ, et non de la date
    -- du calcul. C'est l'écart volontaire avec Distrilog qui rend le mart
    -- rejouable (cf. [NOTES]).
    case
        when date_trunc(date(cc.first_contract_date), year) = date_trunc(m.mois, year)
            then 'GET'
        else 'OTHER'
    end as typologie,
    d.company_economic_model_name as modele_eco_client,
    d.employee_count as effectif,
    d.client_status_name as statut_client,
    d.sector_code_name as code_secteur,
    d.region_name as region,
    d.key_account_name as key_account,
    d.katiers_name as key_account_tiers,
    d.sector_name as secteur_activite,
    d.proadman_name as passage_roadman,

    -- 💶 Chiffre d'affaires
    m.ca_vending_ht_eur,
    m.ca_presta_service_ht_eur,
    m.ca_fontaines_ht_eur,
    m.ca_laves_verres_ht_eur,
    m.ca_nayax_ht_eur,
    m.ca_negoce_ht_eur,
    m.ca_total_ht_eur,

    -- 📦 Coûts produits et marge brute
    m.cout_produits_charges_ht_eur,
    m.cout_produits_livres_ht_eur,
    m.cout_cashless_ht_eur,
    m.cout_produits_total_ht_eur,
    m.marge_brute_eur,
    round(safe_divide(m.marge_brute_eur, m.ca_total_ht_eur) * 100, 2) as pct_marge_brute,

    -- 🔧 Exploitation
    m.nb_min_rm,
    m.cout_rm_eur,
    m.cout_minute_eur,
    m.nb_passages_machine,
    round(safe_divide(m.nb_min_rm, m.nb_passages_machine), 2) as temps_moyen_machine_min,
    m.nb_passages_client,
    round(safe_divide(m.nb_min_rm, m.nb_passages_client), 2) as temps_moyen_client_min,

    -- 🏭 Autres coûts et marge nette
    m.montant_amortissement_eur,
    m.cout_telemetrie_eur,
    m.marge_nette_eur,
    round(safe_divide(m.marge_nette_eur, m.ca_total_ht_eur) * 100, 2) as pct_marge_nette,

    -- 🕒 Date du calcul : le mart reflète l'état courant de l'ERP, qui retouche
    -- des mois clos. Deux exports à des dates différentes peuvent donc différer.
    current_timestamp() as calcule_le

from avec_marges as m
left join `evs-datastack-prod`.`prod_marts`.`dim_neshu__company` as d on m.company_id = d.company_id
left join `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__contrat_client` as cc on m.company_id = cc.company_id
where d.company_code like 'CN%'