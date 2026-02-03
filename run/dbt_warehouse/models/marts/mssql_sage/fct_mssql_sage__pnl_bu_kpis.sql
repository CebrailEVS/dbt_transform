
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`fct_mssql_sage__pnl_bu_kpis`
      
    
    

    
    OPTIONS(
      description="""Table KPI P&L par Business Unit avec sc\u00e9narios avec et sans provisions cong\u00e9s pay\u00e9s. Inclut KPI mensuels, YTD, N-1, \u00e9carts, \u00e9volutions et pourcentages du chiffre d'affaires.\n"""
    )
    as (
      

WITH scenarios AS (
  SELECT 'AVEC_PROVISIONS_CP' AS scenario
  UNION ALL
  SELECT 'SANS_PROVISIONS_CP'
),

base AS (
  SELECT
    s.scenario,
    EXTRACT(YEAR FROM f.date_facturation) AS annee,
    EXTRACT(MONTH FROM f.date_facturation) AS mois,
    COALESCE(f.code_analytique_bu, 'BU_NON_RENSEIGNEE') AS bu,
    f.macro_categorie_pnl_bu,
    f.numero_compte_general,
    f.montant_analytique_signe
  FROM `evs-datastack-prod`.`prod_intermediate`.`int_mssql_sage__pnl_bu` f
  CROSS JOIN scenarios s
  WHERE f.is_missing_analytical = FALSE
    AND EXTRACT(YEAR FROM f.date_facturation) >= 2023
    AND (
      s.scenario = 'AVEC_PROVISIONS_CP'
      OR (
        s.scenario = 'SANS_PROVISIONS_CP'
        AND f.numero_compte_general NOT IN ('645800', '641200')
      )
    )
),

kpi_mensuel AS (
  SELECT
    scenario,
    annee,
    mois,
    bu,

    SUM(CASE WHEN macro_categorie_pnl_bu = "Chiffre d'Affaires"
        THEN montant_analytique_signe ELSE 0 END) AS chiffre_affaire,

    SUM(CASE WHEN macro_categorie_pnl_bu = 'Consommation MP & SSTT'
        THEN montant_analytique_signe ELSE 0 END) AS consommation_mp_sstt,

    SUM(CASE WHEN macro_categorie_pnl_bu = 'Masse Salariale'
        THEN montant_analytique_signe ELSE 0 END) AS masse_salariale,

    SUM(CASE WHEN macro_categorie_pnl_bu = 'Frais Directs & Amortissements'
        THEN montant_analytique_signe ELSE 0 END) AS frais_directs_amortissements

  FROM base
  GROUP BY scenario, annee, mois, bu
),

kpi_calcule AS (
  SELECT
    scenario,
    annee,
    mois,
    bu,

    chiffre_affaire,
    consommation_mp_sstt,
    masse_salariale,
    frais_directs_amortissements,

    chiffre_affaire + consommation_mp_sstt AS marge_brute,
    chiffre_affaire
      + consommation_mp_sstt
      + masse_salariale
      + frais_directs_amortissements AS marge_nette
  FROM kpi_mensuel
),
kpi_ytd AS (
  SELECT
    *,

    SUM(chiffre_affaire) OVER (PARTITION BY scenario, annee, bu ORDER BY mois) AS chiffre_affaire_ytd,
    SUM(consommation_mp_sstt) OVER (PARTITION BY scenario, annee, bu ORDER BY mois) AS consommation_mp_sstt_ytd,
    SUM(masse_salariale) OVER (PARTITION BY scenario, annee, bu ORDER BY mois) AS masse_salariale_ytd,
    SUM(frais_directs_amortissements) OVER (PARTITION BY scenario, annee, bu ORDER BY mois) AS frais_directs_amortissements_ytd,
    SUM(marge_brute) OVER (PARTITION BY scenario, annee, bu ORDER BY mois) AS marge_brute_ytd,
    SUM(marge_nette) OVER (PARTITION BY scenario, annee, bu ORDER BY mois) AS marge_nette_ytd

  FROM kpi_calcule
),
kpi_enrichi AS (
  SELECT
    *,

    -- N-1 mensuel
    LAG(chiffre_affaire) OVER (PARTITION BY scenario, bu, mois ORDER BY annee) AS chiffre_affaire_n_1,
    LAG(consommation_mp_sstt) OVER (PARTITION BY scenario, bu, mois ORDER BY annee) AS consommation_mp_sstt_n_1,
    LAG(masse_salariale) OVER (PARTITION BY scenario, bu, mois ORDER BY annee) AS masse_salariale_n_1,
    LAG(frais_directs_amortissements) OVER (PARTITION BY scenario, bu, mois ORDER BY annee) AS frais_directs_amortissements_n_1,
    LAG(marge_brute) OVER (PARTITION BY scenario, bu, mois ORDER BY annee) AS marge_brute_n_1,
    LAG(marge_nette) OVER (PARTITION BY scenario, bu, mois ORDER BY annee) AS marge_nette_n_1,

    -- ✅ N-1 YTD (SAFE — plus d’imbrication)
    LAG(chiffre_affaire_ytd) OVER (PARTITION BY scenario, bu, mois ORDER BY annee) AS chiffre_affaire_ytd_n_1,
    LAG(consommation_mp_sstt_ytd) OVER (PARTITION BY scenario, bu, mois ORDER BY annee) AS consommation_mp_sstt_ytd_n_1,
    LAG(masse_salariale_ytd) OVER (PARTITION BY scenario, bu, mois ORDER BY annee) AS masse_salariale_ytd_n_1,
    LAG(frais_directs_amortissements_ytd) OVER (PARTITION BY scenario, bu, mois ORDER BY annee) AS frais_directs_amortissements_ytd_n_1,
    LAG(marge_brute_ytd) OVER (PARTITION BY scenario, bu, mois ORDER BY annee) AS marge_brute_ytd_n_1,
    LAG(marge_nette_ytd) OVER (PARTITION BY scenario, bu, mois ORDER BY annee) AS marge_nette_ytd_n_1

  FROM kpi_ytd
),
kpi_long AS (
  SELECT
    scenario,
    annee,
    mois,
    bu,
    kpi,
    valeur,
    valeur_ytd,
    valeur_n_1,
    valeur_ytd_n_1
  FROM kpi_enrichi
  UNPIVOT (
    (valeur, valeur_ytd, valeur_n_1, valeur_ytd_n_1)
    FOR kpi IN (
      (chiffre_affaire, chiffre_affaire_ytd, chiffre_affaire_n_1, chiffre_affaire_ytd_n_1) AS 'CA',
      (consommation_mp_sstt, consommation_mp_sstt_ytd, consommation_mp_sstt_n_1, consommation_mp_sstt_ytd_n_1) AS 'CONSOMMATION_MP_SSTT',
      (masse_salariale, masse_salariale_ytd, masse_salariale_n_1, masse_salariale_ytd_n_1) AS 'MASSE_SALARIALE',
      (frais_directs_amortissements, frais_directs_amortissements_ytd, frais_directs_amortissements_n_1, frais_directs_amortissements_ytd_n_1) AS 'FRAIS_DIRECTS_AMORTISSEMENTS',
      (marge_brute, marge_brute_ytd, marge_brute_n_1, marge_brute_ytd_n_1) AS 'MARGE_BRUTE',
      (marge_nette, marge_nette_ytd, marge_nette_n_1, marge_nette_ytd_n_1) AS 'MARGE_NETTE'
    )
  )
)

SELECT
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
  SAFE_DIVIDE(l.valeur, ca.valeur) AS pct_du_ca,
  SAFE_DIVIDE(l.valeur_ytd, ca.valeur_ytd) AS pct_du_ca_ytd,
  SAFE_DIVIDE(l.valeur_n_1, ca.valeur_n_1) AS pct_du_ca_n_1,
  SAFE_DIVIDE(l.valeur_ytd_n_1, ca.valeur_ytd_n_1) AS pct_du_ca_n_1_ytd,

  -- Écarts mensuels
  l.valeur - l.valeur_n_1 AS ecart_n_vs_n_1,
  SAFE_DIVIDE(l.valeur - l.valeur_n_1, l.valeur_n_1) AS evolution_pct,

  -- KPI YTD
  l.valeur_ytd - l.valeur_ytd_n_1 AS ecart_n_vs_n_1_ytd,
  SAFE_DIVIDE(l.valeur_ytd - l.valeur_ytd_n_1, l.valeur_ytd_n_1) AS evolution_pct_ytd,

  -- Métadonnées dbt
  CURRENT_TIMESTAMP() as dbt_updated_at,
  'c089a5d8-0915-4e5a-8e6f-977253d0467e' as dbt_invocation_id
FROM kpi_long l
LEFT JOIN kpi_long ca
  ON  l.scenario = ca.scenario
  AND l.annee = ca.annee
  AND l.mois = ca.mois
  AND l.bu = ca.bu
  AND ca.kpi = 'CA'
WHERE l.annee >= 2024
ORDER BY annee, mois, bu, kpi
    );
  