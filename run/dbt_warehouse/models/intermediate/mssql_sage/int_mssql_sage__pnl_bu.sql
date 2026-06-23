
  
    

    create or replace table `evs-datastack-prod`.`prod_intermediate`.`int_mssql_sage__pnl_bu`
      
    partition by timestamp_trunc(date_facturation, day)
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Brique de compte de r\u00e9sultat (P&L) par Business Unit : chaque ligne est une \u00e9criture comptable de charge ou de produit, ventil\u00e9e analytiquement et rattach\u00e9e \u00e0 une BU et \u00e0 une macro-cat\u00e9gorie P&L. Base de fct_finance__pnl_bu.\n[COMMENT CONSTRUITE] Jointure des \u00e9critures comptables (stg_mssql_sage__f_ecriturec, filtr\u00e9es aux comptes de classe 6 = charges et 7 = produits) avec les \u00e9critures analytiques (stg_mssql_sage__f_ecriturea). La BU est d\u00e9riv\u00e9e du code analytique via le seed ref_mssql_sage__code_analytique_bu, avec repli sur des pr\u00e9fixes (NUN\u2192NUNSHEN, NES\u2192NESHU, SAV\u2192TECHNIQUE\u2026) ; la macro-cat\u00e9gorie P&L vient du seed ref_mssql_sage__code_comptable_bu. Un remapping historique 2024 (source historic) corrige certaines BU. date_facturation est reconstruite (jm_date + (ec_jour-1) jours).\n[GRAIN] 1 ligne par (numero_ecriture_comptable, numero_plan_analytique, numero_ligne_analytique). ~190k lignes, depuis 2021. Cl\u00e9 strictement unique.\n[NOTES] P\u00e9rim\u00e8tre limit\u00e9 aux comptes 6 et 7 (le bilan, classes 1-5, est exclu). Une \u00e9criture comptable sans ventilation analytique appara\u00eet quand m\u00eame (is_missing_analytical = true, colonnes analytiques NULL). Pour les agr\u00e9gats P&L, sommer montant_analytique_signe.\n"""
    )
    as (
      

with ecritures_comptables as (
    select
        ec_no as numero_ecriture_comptable,
        cg_num as numero_compte_general,
        ec_intitule as libelle_ecriture,
        ec_sens as sens_ecriture,
        ec_date as date_ecriture_comptable,
        jm_date as date_periode_facturation,
        ec_jour as jour_facturation,
        timestamp(date_add(jm_date, interval (ec_jour - 1) day)) as date_facturation,
        created_at,
        updated_at
    from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_ecriturec`
    where
        left(cast(cg_num as string), 1) in ('6', '7')
),

historic_mapping as (
    select *
    from `evs-datastack-prod`.`historic`.`update_mssql_sage__analytique_2024`
),

ecritures_analytiques as (
    select
        ec_no as numero_ecriture_comptable,
        n_analytique as numero_plan_analytique,
        ea_ligne as numero_ligne_analytique,
        ca_num as code_analytique,
        ea_montant as montant_analytique,
        created_at,
        updated_at
    from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_ecriturea`
),

mapping_code_comptable__bu as (
    select *
    from `evs-datastack-prod`.`prod_reference`.`ref_mssql_sage__code_comptable_bu`
),

mapping_code_analytique__bu as (
    select *
    from `evs-datastack-prod`.`prod_reference`.`ref_mssql_sage__code_analytique_bu`
),

mapped_with_fallback as (
    select
        c.numero_ecriture_comptable,
        a.numero_plan_analytique,
        a.numero_ligne_analytique,

        a.code_analytique,
        case
            when bu.code_analytique_bu is not null then bu.code_analytique_bu
            when a.code_analytique like 'NUN%' then 'NUNSHEN'
            when a.code_analytique like 'HOR%' then 'COMMERCE'
            when a.code_analytique like 'OFF%' then 'COMMERCE'
            when a.code_analytique like 'NES%' then 'NESHU'
            when a.code_analytique like 'SAV%' then 'TECHNIQUE'
            when a.code_analytique like 'COM%' then 'COMMERCE'
            when a.code_analytique like 'PDET%' then 'PIECES DET'
        end as code_analytique_bu,

        c.numero_compte_general,
        c.libelle_ecriture,
        cbu.macro_categorie_pnl_bu,

        c.sens_ecriture,

        a.montant_analytique,

        c.date_facturation,
        c.date_ecriture_comptable,
        c.date_periode_facturation,
        c.jour_facturation,

        (a.numero_ecriture_comptable is null) as is_missing_analytical,

        coalesce(a.created_at, c.created_at) as created_at,
        coalesce(a.updated_at, c.updated_at) as updated_at

    from ecritures_comptables as c
    left join ecritures_analytiques as a
        on c.numero_ecriture_comptable = a.numero_ecriture_comptable
    left join mapping_code_analytique__bu as bu
        on a.code_analytique = bu.code_analytique
    left join mapping_code_comptable__bu as cbu
        on cast(c.numero_compte_general as string) = cbu.code_comptable
),

updated_2024 as (
    select
        f.numero_ecriture_comptable,
        f.numero_plan_analytique,
        f.numero_ligne_analytique,
        f.code_analytique,
        coalesce(u.code_analytique_bu, f.code_analytique_bu) as code_analytique_bu,
        f.numero_compte_general,
        f.libelle_ecriture,
        f.macro_categorie_pnl_bu,
        f.sens_ecriture,
        f.montant_analytique,
        f.date_facturation,
        f.date_ecriture_comptable,
        f.date_periode_facturation,
        f.jour_facturation,
        f.is_missing_analytical,
        f.created_at,
        f.updated_at
    from mapped_with_fallback as f
    left join historic_mapping as u
        on
            f.numero_ecriture_comptable = u.numero_ecriture_comptable
            and (
                (
                    f.numero_plan_analytique is not null
                    and f.numero_ligne_analytique is not null
                    and f.numero_plan_analytique = u.numero_plan_analytique
                    and f.numero_ligne_analytique = u.numero_ligne_analytique
                )
                or (
                    f.numero_plan_analytique is null
                    or f.numero_ligne_analytique is null
                )
            )
            and extract(year from f.date_facturation) = 2024
)

select
    numero_ecriture_comptable,
    numero_plan_analytique,
    numero_ligne_analytique,
    code_analytique,
    code_analytique_bu,
    numero_compte_general,
    libelle_ecriture,
    macro_categorie_pnl_bu,
    sens_ecriture,
    montant_analytique,

    case
        when left(cast(numero_compte_general as string), 1) = '6' and sens_ecriture = 0
            then -abs(montant_analytique)
        when left(cast(numero_compte_general as string), 1) = '6' and sens_ecriture = 1
            then abs(montant_analytique)
        when left(cast(numero_compte_general as string), 1) = '7' and sens_ecriture = 0
            then -abs(montant_analytique)
        when left(cast(numero_compte_general as string), 1) = '7' and sens_ecriture = 1
            then abs(montant_analytique)
        else montant_analytique
    end as montant_analytique_signe,

    date_facturation,
    date_ecriture_comptable,
    date_periode_facturation,
    jour_facturation,
    is_missing_analytical,

    (
        code_analytique_bu is null
        and is_missing_analytical = false
    ) as is_missing_bu_mapping,

    (macro_categorie_pnl_bu is null) as is_missing_comptable_mapping,

    created_at,
    updated_at

from updated_2024
    );
  