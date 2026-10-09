
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`fct_finance__pnl_section_mensuel`
      
    
    cluster by code_analytique, categorie_pnl_bu

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] P&L mensuel au niveau le plus fin du pilotage analytique : par section analytique Sage et par compte g\u00e9n\u00e9ral, avec sa cat\u00e9gorie et sa macro-cat\u00e9gorie P&L. Permet de suivre un poste (ex. masse salariale) pour une \u00e9quipe donn\u00e9e (ex. NESOOOSAV, roadmen SAV Neshu).\n[COMMENT CONSTRUITE] int_mssql_sage__pnl_bu (\u00e9critures classes 6/7 ventil\u00e9es, signe Sage conserv\u00e9) agr\u00e9g\u00e9 par mois de facturation, section, BU et compte ; intitul\u00e9 de section depuis stg_mssql_sage__f_comptea (plan 1) ; cat\u00e9gories depuis le seed ref_mssql_sage__code_comptable_bu.\n[GRAIN] 1 ligne par (mois_date, code_analytique, code_analytique_bu, numero_compte_general). ~96k lignes depuis 2021. La BU est dans le grain : le remapping historique 2024 rattache certaines \u00e9critures d'une m\u00eame section \u00e0 deux BU.\n[NOTES] Exclut les \u00e9critures sans ventilation analytique (pas de section) : elles restent dans fct_finance__pnl_bu (BU_NON_RENSEIGNEE). Plan analytique 1 uniquement. Pas de sc\u00e9nario avec/sans provisions CP : filtrer les comptes 641200 / 645800 si besoin. Le mois courant est partiel. Pas de dimension section ni compte : codes port\u00e9s en clair (pas de test relationships). Filtrer un poste sur categorie_pnl_bu, pas sur la macro : la cat\u00e9gorie Variable est rang\u00e9e sous \u00ab Frais Directs & Amortissements \u00bb dans le seed.\n"""
    )
    as (
      

with ecritures as (
    select
        date(date_trunc(date_facturation, month)) as mois_date,
        code_analytique,
        coalesce(code_analytique_bu, 'BU_NON_RENSEIGNEE') as code_analytique_bu,
        numero_compte_general,
        categorie_pnl_bu,
        macro_categorie_pnl_bu,
        montant_analytique_signe
    from `evs-datastack-prod`.`prod_intermediate`.`int_mssql_sage__pnl_bu`
    -- Sans ventilation analytique, pas de section : ces écritures restent dans fct_finance__pnl_bu.
    -- Plan 1 explicite : un 2e plan analytique Sage doublerait les montants sans casser le grain.
    where
        not is_missing_analytical
        and numero_plan_analytique = 1
),

sections as (
    select
        ca_num as code_analytique,
        ca_intitule as section_analytique_intitule
    from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_comptea`
    where n_analytique = 1
)

select
    e.mois_date,
    e.code_analytique,
    e.code_analytique_bu,
    e.numero_compte_general,
    s.section_analytique_intitule,
    e.categorie_pnl_bu,
    e.macro_categorie_pnl_bu,
    sum(e.montant_analytique_signe) as montant_signe
from ecritures as e
left join sections as s
    on e.code_analytique = s.code_analytique
group by
    e.mois_date,
    e.code_analytique,
    e.code_analytique_bu,
    e.numero_compte_general,
    s.section_analytique_intitule,
    e.categorie_pnl_bu,
    e.macro_categorie_pnl_bu
    );
  