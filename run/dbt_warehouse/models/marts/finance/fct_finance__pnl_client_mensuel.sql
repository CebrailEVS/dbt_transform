
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`fct_finance__pnl_client_mensuel`
      
    partition by date_trunc(mois, month)
    cluster by company_code

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Compte de r\u00e9sultat mensuel par client Neshu (codes CN), du chiffre d'affaires \u00e0 la marge nette. Reprise du rapport que les d\u00e9veloppeurs de Distrilog produisent par une requ\u00eate SQL de 457 lignes, port\u00e9e dans notre stack.\n[COMMENT CONSTRUITE] Assemble int_oracle_neshu__ca_client_mensuel (6 composantes de CA), __cout_produits_mensuel (produits charg\u00e9s et livr\u00e9s), __temps_appro_mensuel (temps et passages), __charges_sociales (enveloppe \u00e0 r\u00e9partir), __amortissement_machines, __telemetrie_parc et __contrat_client, enrichi de la signal\u00e9tique de dim_neshu__company. Cha\u00eenage : CA \u2212 co\u00fbts produits = marge brute ; \u2212 amortissement \u2212 co\u00fbt de main-d'oeuvre \u2212 t\u00e9l\u00e9m\u00e9trie = marge nette.\n[GRAIN] 1 ligne par (mois, company_id), restreint aux clients dont le code commence par CN.\n[NOTES] \u00c9CARTS CONNUS AVEC LE RAPPORT DISTRILOG \u2014 \u00e0 lire avant de conclure \u00e0 une erreur de calcul. Validation du 2026-09-15 sur ao\u00fbt 2026, 180 clients.\n  (1) EXACTS \u00c0 180/180 : CA Nayax, CA n\u00e9goce, CA lave-verres, co\u00fbt des produits charg\u00e9s, co\u00fbt des produits livr\u00e9s, co\u00fbt cashless, temps RM, co\u00fbt RM, co\u00fbt minute, passages machines et clients, temps moyens, co\u00fbt t\u00e9l\u00e9m\u00e9trie.\n  (2) LATENCE DE PURGE (177/180, +566 \u20ac sur le CA total d'ao\u00fbt) : le raw conserve les lignes supprim\u00e9es dans l'ERP jusqu'\u00e0 la purge du dimanche (CONVENTIONS.md r\u00e8gle 9, latence accept\u00e9e \u2264 7 jours). L'\u00e9cart cro\u00eet du dimanche au samedi puis retombe. Il touche le CA vending, presta et fontaines, donc la marge brute. Ce n'est pas un d\u00e9faut de mod\u00e8le : c'est le d\u00e9lai de propagation des suppressions.\n  (3) AMORTISSEMENT (97/180, +841 \u20ac sur ao\u00fbt 2026) : \u00e9cart VOLONTAIRE et \u00e9lucid\u00e9 le 2026-09-15. La requ\u00eate Distrilog groupe les sous-\u00e9quipements par la VALEUR de leur dotation et non par leur identifiant, ce qui produit deux d\u00e9fauts oppos\u00e9s. Sur 105 machines dont les sous-\u00e9quipements ont la m\u00eame dotation, ils sont fusionn\u00e9s en un seul groupe : trois accessoires n'en comptent qu'un, la dotation est SOUS-\u00e9valu\u00e9e. Sur 662 machines dont les sous-\u00e9quipements ont des dotations distinctes, chaque valeur cr\u00e9e un groupe portant AUSSI la dotation de la machine, compt\u00e9e autant de fois : elle est SUR-\u00e9valu\u00e9e. Les deux effets se compensent partiellement, le net vaut \u2212841 \u20ac face \u00e0 un calcul correct. Ici chaque sous-\u00e9quipement compte une fois et chaque machine une fois (agr\u00e9gation avant jointure). Ce mod\u00e8le est donc plus juste que le rapport d'origine, et la marge nette h\u00e9rite de l'\u00e9cart. S'y ajoute, marginalement, la latence de purge du point (2), qui fait entrer des machines vues sur des t\u00e2ches depuis supprim\u00e9es.\n  (4) TYPOLOGIE, \u00e9cart volontaire : un client est GET s'il a sign\u00e9 dans l'ann\u00e9e DU MOIS ANALYS\u00c9. Distrilog compare \u00e0 l'ann\u00e9e du jour o\u00f9 la requ\u00eate tourne, ce qui fait basculer l'historique chaque 1er janvier. Le choix d'ici rend le mart rejouable.\n  (5) FUITE DE R\u00c9PARTITION, reproduite volontairement : le d\u00e9nominateur du co\u00fbt RM couvre tous les clients ayant re\u00e7u un passage, y compris ceux absents du rapport faute de CA. La somme des co\u00fbts RM affich\u00e9s est donc inf\u00e9rieure aux charges saisies \u2014 87 \u20ac sur 153 946 \u20ac en juin 2026, soit 0,06 %. Corriger le d\u00e9nominateur ferait diverger tous les clients de Distrilog pour r\u00e9cup\u00e9rer 0,06 % : non retenu.\n  (6) CO\u00dbT RM \u00c0 Z\u00c9RO : les charges sociales sont saisies manuellement dans l'ERP, une t\u00e2che par mois. Rien n'est saisi depuis juin 2026, donc cout_rm_eur vaut 0 sur juillet et ao\u00fbt et la marge nette est sur\u00e9valu\u00e9e d'autant. Le rapport Distrilog porte le m\u00eame trou. Donn\u00e9e absente \u00e0 la source, pas erreur de calcul.\n  (7) Le mart RECALCULE tout \u00e0 chaque run : l'ERP retouche des mois clos (1,4 % des t\u00e2ches de juillet 2026 modifi\u00e9es apr\u00e8s sa cl\u00f4ture). Un mois publi\u00e9 peut donc changer. La colonne calcule_le date le calcul ; deux exports qui diff\u00e8rent s'expliquent par l\u00e0.\n  Taux de co\u00fbt cashless : var('taux_cout_cashless'), 2 % comme chez Distrilog, sans trace de ce taux en base.\n"""
    )
    as (
      

-- Compte de résultat mensuel par client Neshu, reprise du rapport que les
-- développeurs de Distrilog produisent par une requête SQL de 457 lignes.
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
        ca.company_name,

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
    m.company_code,
    m.company_name,

    -- 📇 Signalétique client, portée par la dimension
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
where m.company_code like 'CN%'
    );
  