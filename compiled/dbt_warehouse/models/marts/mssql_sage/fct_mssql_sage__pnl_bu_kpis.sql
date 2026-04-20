

with scenarios as (
-- Scénarios
    select 'AVEC_PROVISIONS_CP' as scenario
    union all
    select 'SANS_PROVISIONS_CP'
),

base as (
-- Base réel
    select
        s.scenario,
        extract(year from f.date_facturation) as annee,
        extract(month from f.date_facturation) as mois,
        coalesce(f.code_analytique_bu, 'BU_NON_RENSEIGNEE') as bu,
        f.macro_categorie_pnl_bu,
        f.numero_compte_general,
        f.montant_analytique_signe
    from `evs-datastack-prod`.`prod_intermediate`.`int_mssql_sage__pnl_bu` as f
    cross join scenarios as s
    where
        f.is_missing_analytical = false
        and extract(year from f.date_facturation) >= 2023
        and (
            s.scenario = 'AVEC_PROVISIONS_CP'
            or (s.scenario = 'SANS_PROVISIONS_CP' and f.numero_compte_general not in ('645800', '641200'))
        )
),

kpi_mensuel as (
-- KPI mensuels réel
    select
        scenario,
        annee,
        mois,
        bu,
        sum(case when macro_categorie_pnl_bu = "Chiffre d'Affaires" then montant_analytique_signe else 0 end)
            as chiffre_affaire,
        sum(case when macro_categorie_pnl_bu = 'Consommation MP & SSTT' then montant_analytique_signe else 0 end)
            as consommation_mp_sstt,
        sum(case when macro_categorie_pnl_bu = 'Masse Salariale' then montant_analytique_signe else 0 end)
            as masse_salariale,
        sum(
            case when macro_categorie_pnl_bu = 'Frais Directs & Amortissements' then montant_analytique_signe else 0 end
        ) as frais_directs_amortissements
    from base
    group by scenario, annee, mois, bu
),

kpi_calcule as (
-- KPI dérivés réel
    select
        scenario,
        annee,
        mois,
        bu,
        chiffre_affaire,
        consommation_mp_sstt,
        masse_salariale,
        frais_directs_amortissements,
        chiffre_affaire + consommation_mp_sstt as marge_brute,
        chiffre_affaire + consommation_mp_sstt + masse_salariale + frais_directs_amortissements as marge_nette
    from kpi_mensuel
),

kpi_ytd as (
-- YTD réel
    select
        scenario,
        annee,
        mois,
        bu,
        chiffre_affaire,
        consommation_mp_sstt,
        masse_salariale,
        frais_directs_amortissements,
        marge_brute,
        marge_nette,
        sum(chiffre_affaire) over (partition by scenario, annee, bu order by mois) as chiffre_affaire_ytd,
        sum(consommation_mp_sstt) over (partition by scenario, annee, bu order by mois) as consommation_mp_sstt_ytd,
        sum(masse_salariale) over (partition by scenario, annee, bu order by mois) as masse_salariale_ytd,
        sum(frais_directs_amortissements)
            over (partition by scenario, annee, bu order by mois)
            as frais_directs_amortissements_ytd,
        sum(marge_brute) over (partition by scenario, annee, bu order by mois) as marge_brute_ytd,
        sum(marge_nette) over (partition by scenario, annee, bu order by mois) as marge_nette_ytd
    from kpi_calcule
),

kpi_enrichi as (
-- N-1 réel
    select
        scenario,
        annee,
        mois,
        bu,
        chiffre_affaire,
        consommation_mp_sstt,
        masse_salariale,
        frais_directs_amortissements,
        marge_brute,
        marge_nette,
        chiffre_affaire_ytd,
        consommation_mp_sstt_ytd,
        masse_salariale_ytd,
        frais_directs_amortissements_ytd,
        marge_brute_ytd,
        marge_nette_ytd,
        lag(chiffre_affaire) over (partition by scenario, bu, mois order by annee) as chiffre_affaire_n_1,
        lag(consommation_mp_sstt) over (partition by scenario, bu, mois order by annee) as consommation_mp_sstt_n_1,
        lag(masse_salariale) over (partition by scenario, bu, mois order by annee) as masse_salariale_n_1,
        lag(frais_directs_amortissements)
            over (partition by scenario, bu, mois order by annee)
            as frais_directs_amortissements_n_1,
        lag(marge_brute) over (partition by scenario, bu, mois order by annee) as marge_brute_n_1,
        lag(marge_nette) over (partition by scenario, bu, mois order by annee) as marge_nette_n_1,
        lag(chiffre_affaire_ytd) over (partition by scenario, bu, mois order by annee) as chiffre_affaire_ytd_n_1,
        lag(consommation_mp_sstt_ytd)
            over (partition by scenario, bu, mois order by annee)
            as consommation_mp_sstt_ytd_n_1,
        lag(masse_salariale_ytd) over (partition by scenario, bu, mois order by annee) as masse_salariale_ytd_n_1,
        lag(frais_directs_amortissements_ytd)
            over (partition by scenario, bu, mois order by annee)
            as frais_directs_amortissements_ytd_n_1,
        lag(marge_brute_ytd) over (partition by scenario, bu, mois order by annee) as marge_brute_ytd_n_1,
        lag(marge_nette_ytd) over (partition by scenario, bu, mois order by annee) as marge_nette_ytd_n_1
    from kpi_ytd
),

kpi_long as (
-- Format long réel
    select
        scenario,
        annee,
        mois,
        bu,
        kpi,
        valeur,
        valeur_ytd,
        valeur_n_1,
        valeur_ytd_n_1
    from kpi_enrichi
    unpivot ((valeur, valeur_ytd, valeur_n_1, valeur_ytd_n_1) for kpi in (
        (chiffre_affaire, chiffre_affaire_ytd, chiffre_affaire_n_1, chiffre_affaire_ytd_n_1) as 'CA',
        (consommation_mp_sstt, consommation_mp_sstt_ytd, consommation_mp_sstt_n_1, consommation_mp_sstt_ytd_n_1)
            as 'CONSOMMATION_MP_SSTT',
        (masse_salariale, masse_salariale_ytd, masse_salariale_n_1, masse_salariale_ytd_n_1) as 'MASSE_SALARIALE',
        (
            frais_directs_amortissements,
            frais_directs_amortissements_ytd,
            frais_directs_amortissements_n_1,
            frais_directs_amortissements_ytd_n_1
        ) as 'FRAIS_DIRECTS_AMORTISSEMENTS',
        (marge_brute, marge_brute_ytd, marge_brute_n_1, marge_brute_ytd_n_1) as 'MARGE_BRUTE',
        (marge_nette, marge_nette_ytd, marge_nette_n_1, marge_nette_ytd_n_1) as 'MARGE_NETTE'
    ))
),

budget_base as (
-- Budget normalisé
    select
        budg_annee as annee,
        budg_mois as mois,
        budg_bu as bu,
        case
            when budg_categorie_pnl = 'CA' then "Chiffre d'Affaires"
            when budg_categorie_pnl = 'CONSOMMATION_MP_SSTT' then 'Consommation MP & SSTT'
            when budg_categorie_pnl = 'MASSE_SALARIALE' then 'Masse Salariale'
        end as macro_categorie_pnl_bu,
        budg_valeur as montant_analytique_signe
    from `evs-datastack-prod`.`prod_reference`.`ref_mssql_sage__pnl_budget`
),

budget_kpi_mensuel as (
-- KPI budget
    select
        annee,
        mois,
        bu,
        sum(case when macro_categorie_pnl_bu = "Chiffre d'Affaires" then montant_analytique_signe else 0 end)
            as chiffre_affaire,
        sum(case when macro_categorie_pnl_bu = 'Consommation MP & SSTT' then montant_analytique_signe else 0 end)
            as consommation_mp_sstt,
        sum(case when macro_categorie_pnl_bu = 'Masse Salariale' then montant_analytique_signe else 0 end)
            as masse_salariale,
        cast(0 as float64) as frais_directs_amortissements
    from budget_base
    group by annee, mois, bu
),

budget_kpi_calcule as (
-- KPI dérivés budget
    select
        annee,
        mois,
        bu,
        chiffre_affaire,
        consommation_mp_sstt,
        masse_salariale,
        frais_directs_amortissements,
        chiffre_affaire + consommation_mp_sstt as marge_brute,
        chiffre_affaire + consommation_mp_sstt + masse_salariale as marge_nette
    from budget_kpi_mensuel
),

budget_kpi_ytd as (
-- YTD budget
    select
        annee,
        mois,
        bu,
        chiffre_affaire,
        consommation_mp_sstt,
        masse_salariale,
        frais_directs_amortissements,
        marge_brute,
        marge_nette,
        sum(chiffre_affaire) over (partition by annee, bu order by mois) as chiffre_affaire_ytd,
        sum(consommation_mp_sstt) over (partition by annee, bu order by mois) as consommation_mp_sstt_ytd,
        sum(masse_salariale) over (partition by annee, bu order by mois) as masse_salariale_ytd,
        sum(frais_directs_amortissements)
            over (partition by annee, bu order by mois)
            as frais_directs_amortissements_ytd,
        sum(marge_brute) over (partition by annee, bu order by mois) as marge_brute_ytd,
        sum(marge_nette) over (partition by annee, bu order by mois) as marge_nette_ytd
    from budget_kpi_calcule
),

budget_long as (
-- Format long budget
    select
        annee,
        mois,
        bu,
        kpi,
        valeur,
        valeur_ytd
    from budget_kpi_ytd
    unpivot ((valeur, valeur_ytd) for kpi in (
        (chiffre_affaire, chiffre_affaire_ytd) as 'CA',
        (consommation_mp_sstt, consommation_mp_sstt_ytd) as 'CONSOMMATION_MP_SSTT',
        (masse_salariale, masse_salariale_ytd) as 'MASSE_SALARIALE',
        (frais_directs_amortissements, frais_directs_amortissements_ytd) as 'FRAIS_DIRECTS_AMORTISSEMENTS',
        (marge_brute, marge_brute_ytd) as 'MARGE_BRUTE',
        (marge_nette, marge_nette_ytd) as 'MARGE_NETTE'
    ))
),

kpi_with_budget as (
-- Enrichissement final
    select
        l.scenario,
        l.annee,
        l.mois,
        l.bu,
        l.kpi,
        l.valeur,
        l.valeur_ytd,
        l.valeur_n_1,
        l.valeur_ytd_n_1,
        b.valeur as budget,
        b.valeur_ytd as budget_ytd,
        safe_divide(l.valeur, ca.valeur) as pct_du_ca,
        safe_divide(l.valeur_ytd, ca.valeur_ytd) as pct_du_ca_ytd,
        safe_divide(l.valeur_n_1, ca.valeur_n_1) as pct_du_ca_n_1,
        safe_divide(l.valeur_ytd_n_1, ca.valeur_ytd_n_1) as pct_du_ca_n_1_ytd,
        l.valeur - l.valeur_n_1 as ecart_n_vs_n_1,
        safe_divide(l.valeur - l.valeur_n_1, l.valeur_n_1) as evolution_pct,
        l.valeur_ytd - l.valeur_ytd_n_1 as ecart_n_vs_n_1_ytd,
        safe_divide(l.valeur_ytd - l.valeur_ytd_n_1, l.valeur_ytd_n_1) as evolution_pct_ytd,
        l.valeur - b.valeur as ecart_vs_budget,
        safe_divide(l.valeur - b.valeur, b.valeur) as ecart_vs_budget_pct,
        l.valeur_ytd - b.valeur_ytd as ecart_vs_budget_ytd,
        safe_divide(l.valeur_ytd - b.valeur_ytd, b.valeur_ytd) as ecart_vs_budget_pct_ytd
    from kpi_long as l
    left join
        kpi_long as ca
        on l.scenario = ca.scenario and l.annee = ca.annee and l.mois = ca.mois and l.bu = ca.bu and ca.kpi = 'CA'
    left join budget_long as b on l.annee = b.annee and l.mois = b.mois and l.bu = b.bu and l.kpi = b.kpi
)

select
    scenario,
    annee,
    mois,
    bu,
    kpi,
    valeur,
    valeur_ytd,
    valeur_n_1,
    valeur_ytd_n_1,
    budget,
    budget_ytd,
    pct_du_ca,
    pct_du_ca_ytd,
    pct_du_ca_n_1,
    pct_du_ca_n_1_ytd,
    ecart_n_vs_n_1,
    evolution_pct,
    ecart_n_vs_n_1_ytd,
    evolution_pct_ytd,
    ecart_vs_budget,
    ecart_vs_budget_pct,
    ecart_vs_budget_ytd,
    ecart_vs_budget_pct_ytd,
    current_timestamp() as dbt_updated_at,
    '734e55d9-3171-45df-b455-232e706788e5' as dbt_invocation_id
from kpi_with_budget
where annee >= 2024
order by annee, mois, bu, kpi