
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__technique_rupture_depot_jour_vide`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Jours o\u00f9 un d\u00e9p\u00f4t TechCare n'a plus une r\u00e9f\u00e9rence en stock (seuls jours susceptibles d'\u00eatre une rupture de service) : num\u00e9rateur des taux de disponibilit\u00e9 de Cockpit Supply (tendance d\u00e9p\u00f4t, disponibilit\u00e9 par article, CODIR).\n[COMMENT CONSTRUITE] fct_supply_chain__rupture_depot_yuman depuis le 1er janvier N-1, r\u00e9f\u00e9rences de fournisseur connu (macro cockpit_fournisseur_rupture_tech), code article (macro cockpit_code_article_rupture_tech), lignes \u00e0 qty_depot <= 0 seulement.\n[GRAIN] 1 ligne par (stock_date, depot, reference).\n[NOTES] La r\u00e8gle de l'app (qualifier_rupture_tech) ne compte jamais en rupture un jour o\u00f9 le d\u00e9p\u00f4t a du stock : ces jours ne servent qu'au d\u00e9nominateur, compt\u00e9 dans app_cockpit__technique_rupture_depot_mois. L'app qualifie ces lignes (pr\u00e9sence dans les vans actifs, filtre d'activit\u00e9, pi\u00e8ces interdites). R\u00e9serv\u00e9 \u00e0 l'application.\n"""
    )
    as (
      

-- Historique des ruptures TechCare, partie détaillée : seulement les jours où le dépôt
-- est à zéro (qty_depot <= 0), les seuls qui peuvent compter en rupture (règle
-- qualifier_rupture_tech de l'app : stock dépôt > 0 → jamais en rupture). Les autres
-- jours ne servent qu'au dénominateur du taux : ils sont comptés dans
-- app_cockpit__technique_rupture_depot_mois. Depuis le 1er janvier N-1 (fenêtre de
-- l'app), références de fournisseur connu, fournisseur et code article calculés comme
-- l'app (préfixes, sensible à la casse).
with lignes as (
    select
        stock_date,
        depot,
        trim(reference) as reference,
        coalesce(qty_depot, 0) as qty_depot,
        coalesce(qty_vans_depot, 0) as qty_vans_depot,
        coalesce(is_out_of_stock_depot, false) as is_out_of_stock_depot
    from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__rupture_depot_yuman`
    where stock_date >= date_trunc(date_sub(current_date('Europe/Paris'), interval 1 year), year)
),
normalisees as (
    select
        *,
        case
    when starts_with(reference, 'EVS_NESPRESSO_') or starts_with(reference, 'EVS_NESP_') then 'Nespresso'
    when starts_with(reference, 'EVS_NESTLE_') then 'Nestlé'
    when starts_with(reference, 'ANIM_') then 'Animo'
    when starts_with(reference, 'EVS_BRITA_') then 'Brita'
    when starts_with(reference, 'BRIT') then 'Brita_GF'
    when starts_with(reference, 'TWYD') or starts_with(reference, 'TYWD') then 'TYWD'
    when starts_with(reference, 'AUUM_') then 'AUUM'
end as fournisseur
    from lignes
)
select
    stock_date,
    depot,
    reference,
    fournisseur,
    upper(case
    when fournisseur = 'Nespresso' then replace(replace(reference, 'EVS_NESPRESSO_', ''), 'EVS_NESP_', '')
    when fournisseur = 'Nestlé' then replace(reference, 'EVS_NESTLE_', '')
    else reference
end) as code_article,
    qty_depot,
    qty_vans_depot,
    is_out_of_stock_depot
from normalisees
where fournisseur is not null and qty_depot <= 0
    );
  