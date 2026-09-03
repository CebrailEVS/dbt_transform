
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`fct_bi__consultation`
      
    partition by consultation_date
    cluster by report_id, user_id

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Journal des consultations du parc de rapports Power BI, au grain le plus fin : qui a ouvert quel rapport, quand, et depuis quel client. R\u00e9pond \u00e0 \u00ab qui utilise quoi \u00bb, l\u00e0 o\u00f9 les deux autres faits ne r\u00e9pondent qu'\u00e0 \u00ab combien \u00bb.\n[COMMENT CONSTRUITE] stg_powerbi_activity__events restreint \u00e0 `activity = 'ViewReport'`, en jointure interne sur dim_bi__rapport. La jointure interne \u00e9carte les consultations hors p\u00e9rim\u00e8tre m\u00e9tier (2 sur 248 au 2026-09-03 : copies d'App ou rapports de m\u00e9triques d'usage). Aplatit `report_name` et `workspace_name` du rapport parent ; `user_domain` est d\u00e9riv\u00e9 de l'UPN.\n[GRAIN] 1 ligne par event_id (PK) = 1 consultation par une personne. 246 lignes au 2026-09-03, pour 32 lecteurs distincts.\n[NOTES] \u26a0\ufe0f CONTIENT DES DONN\u00c9ES PERSONNELLES NOMINATIVES (`user_id`) \u2014 arbitrage du propri\u00e9taire du d\u00e9p\u00f4t le 2026-09-03, cf. l'en-t\u00eate de ce fichier. Fait sans mesure (\u00ab factless fact \u00bb) : le d\u00e9compte des consultations est le `count(*)` des lignes. Toutes les consultations sont en `user_type = 0`, c'est-\u00e0-dire des personnes r\u00e9elles \u2014 aucun compte de service n'ouvre de rapport, ce qui rend ce fait enti\u00e8rement humain. `client_ip` et `user_agent` ne sont pas remont\u00e9s \u00e0 dessein. Profondeur born\u00e9e par la source : l'API ne conserve que 27 jours glissants et prod_raw est la seule archive.\n"""
    )
    as (
      

with parc as (
    select
        report_id,
        workspace_id,
        dataset_id,
        report_name,
        workspace_name
    from `evs-datastack-prod`.`prod_marts`.`dim_bi__rapport`
),

consultations as (
    select
        event_id,
        report_id,
        user_id,
        created_at,
        consumption_method,
        distribution_method
    from `evs-datastack-prod`.`prod_staging`.`stg_powerbi_activity__events`
    where activity = 'ViewReport'
)

select
    -- grain : 1 ligne = 1 consultation d'un rapport par une personne
    date(c.created_at) as consultation_date,
    c.event_id,

    -- cles etrangeres
    c.report_id,
    p.workspace_id,
    p.dataset_id,

    -- attributs d'affichage aplatis du parent direct (marts.md § 1)
    p.report_name,
    p.workspace_name,

    -- QUI : dimension degeneree portee par le fait. 32 lecteurs distincts pour
    -- 2 domaines, ce qui ne justifie pas une dim_bi__utilisateur dediee.
    -- Donnee personnelle : voir l'en-tete de _bi__marts_models.yml.
    c.user_id,
    split(c.user_id, '@')[safe_offset(1)] as user_domain,

    -- COMMENT
    c.consumption_method,
    c.distribution_method,

    -- horodatage precis de la consultation
    c.created_at as consultation_at

from consultations as c
-- INNER JOIN : restreint aux consultations du parc metier. Ecarte les 2 vues
-- portant sur un rapport hors perimetre (copie d'App ou metriques d'usage).
inner join parc as p on c.report_id = p.report_id
    );
  