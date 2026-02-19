

with scenarios as (
    select 'AVEC_PROVISIONS_CP' as scenario
    union all
    select 'SANS_PROVISIONS_CP'
),

base as (
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
            or (
                s.scenario = 'SANS_PROVISIONS_CP'
                and f.numero_compte_general not in ('645800', '641200')
            )
        )
),

kpi_mensuel as (
    select
        scenario,
        annee,
        mois,
        bu,

        sum(case
            when macro_categorie_pnl_bu = "Chiffre d'Affaires"
                then montant_analytique_signe
            else 0
        end) as chiffre_affaire,

        sum(case
            when macro_categorie_pnl_bu = 'Consommation MP & SSTT'
                then montant_analytique_signe
            else 0
        end) as consommation_mp_sstt,

        sum(case
            when macro_categorie_pnl_bu = 'Masse Salariale'
                then montant_analytique_signe
            else 0
        end) as masse_salariale,

        sum(case
            when macro_categorie_pnl_bu = 'Frais Directs & Amortissements'
                then montant_analytique_signe
            else 0
        end) as frais_directs_amortissements

    from base
    group by scenario, annee, mois, bu
),

kpi_calcule as (
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
        chiffre_affaire
        + consommation_mp_sstt
        + masse_salariale
        + frais_directs_amortissements as marge_nette
    from kpi_mensuel
),

kpi_ytd as (
    select
        *,

        sum(chiffre_affaire) over (partition by scenario, annee, bu order by mois) as chiffre_affaire_ytd,
        sum(consommation_mp_sstt) over (partition by scenario, annee, bu order by mois) as consommation_mp_sstt_ytd,
        sum(masse_salariale) over (partition by scenario, annee, bu order by mois) as masse_salariale_ytd,
        sum(frais_directs_amortissements) over (
            partition by scenario, annee, bu order by mois
        ) as frais_directs_amortissements_ytd,
        sum(marge_brute) over (partition by scenario, annee, bu order by mois) as marge_brute_ytd,
        sum(marge_nette) over (partition by scenario, annee, bu order by mois) as marge_nette_ytd

    from kpi_calcule
),

kpi_enrichi as (
    select
        *,

        -- N-1 mensuel
        lag(chiffre_affaire) over (partition by scenario, bu, mois order by annee) as chiffre_affaire_n_1,
        lag(consommation_mp_sstt) over (partition by scenario, bu, mois order by annee) as consommation_mp_sstt_n_1,
        lag(masse_salariale) over (partition by scenario, bu, mois order by annee) as masse_salariale_n_1,
        lag(frais_directs_amortissements) over (
            partition by scenario, bu, mois order by annee
        ) as frais_directs_amortissements_n_1,
        lag(marge_brute) over (partition by scenario, bu, mois order by annee) as marge_brute_n_1,
        lag(marge_nette) over (partition by scenario, bu, mois order by annee) as marge_nette_n_1,

        -- N-1 YTD
        lag(chiffre_affaire_ytd) over (partition by scenario, bu, mois order by annee) as chiffre_affaire_ytd_n_1,
        lag(consommation_mp_sstt_ytd) over (
            partition by scenario, bu, mois order by annee
        ) as consommation_mp_sstt_ytd_n_1,
        lag(masse_salariale_ytd) over (partition by scenario, bu, mois order by annee) as masse_salariale_ytd_n_1,
        lag(frais_directs_amortissements_ytd) over (
            partition by scenario, bu, mois order by annee
        ) as frais_directs_amortissements_ytd_n_1,
        lag(marge_brute_ytd) over (partition by scenario, bu, mois order by annee) as marge_brute_ytd_n_1,
        lag(marge_nette_ytd) over (partition by scenario, bu, mois order by annee) as marge_nette_ytd_n_1

    from kpi_ytd
),

kpi_long as (
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
    unpivot (
        (valeur, valeur_ytd, valeur_n_1, valeur_ytd_n_1)
        for kpi in (
            (chiffre_affaire, chiffre_affaire_ytd, chiffre_affaire_n_1, chiffre_affaire_ytd_n_1) as 'CA',
            (
                consommation_mp_sstt, consommation_mp_sstt_ytd,
                consommation_mp_sstt_n_1, consommation_mp_sstt_ytd_n_1
            ) as 'CONSOMMATION_MP_SSTT',
            (masse_salariale, masse_salariale_ytd, masse_salariale_n_1, masse_salariale_ytd_n_1) as 'MASSE_SALARIALE',
            (
                frais_directs_amortissements, frais_directs_amortissements_ytd,
                frais_directs_amortissements_n_1,
                frais_directs_amortissements_ytd_n_1
            ) as 'FRAIS_DIRECTS_AMORTISSEMENTS',
            (marge_brute, marge_brute_ytd, marge_brute_n_1, marge_brute_ytd_n_1) as 'MARGE_BRUTE',
            (marge_nette, marge_nette_ytd, marge_nette_n_1, marge_nette_ytd_n_1) as 'MARGE_NETTE'
        )
    )
)

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

    -- % du CA
    safe_divide(l.valeur, ca.valeur) as pct_du_ca,
    safe_divide(l.valeur_ytd, ca.valeur_ytd) as pct_du_ca_ytd,
    safe_divide(l.valeur_n_1, ca.valeur_n_1) as pct_du_ca_n_1,
    safe_divide(l.valeur_ytd_n_1, ca.valeur_ytd_n_1) as pct_du_ca_n_1_ytd,

    -- Ecarts mensuels
    l.valeur - l.valeur_n_1 as ecart_n_vs_n_1,
    safe_divide(l.valeur - l.valeur_n_1, l.valeur_n_1) as evolution_pct,

    -- KPI YTD
    l.valeur_ytd - l.valeur_ytd_n_1 as ecart_n_vs_n_1_ytd,
    safe_divide(l.valeur_ytd - l.valeur_ytd_n_1, l.valeur_ytd_n_1) as evolution_pct_ytd,

    -- Metadonnees dbt
    current_timestamp() as dbt_updated_at,
    '5742a344-3c0e-439a-81b7-782c57ee2f09' as dbt_invocation_id

from kpi_long as l
left join kpi_long as ca
    on
        l.scenario = ca.scenario
        and l.annee = ca.annee
        and l.mois = ca.mois
        and l.bu = ca.bu
        and ca.kpi = 'CA'
where l.annee >= 2024
order by l.annee, l.mois, l.bu, l.kpi