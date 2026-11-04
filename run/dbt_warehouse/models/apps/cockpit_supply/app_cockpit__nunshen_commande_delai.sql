
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__nunshen_commande_delai`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Commandes clients Nunshen (bons de commande Sage, intragroupe compris) et date de leurs \u00e9tapes, pour les indicateurs ISO de traitement des commandes : pr\u00e9paration lanc\u00e9e (BC \u2192 pr\u00e9paration de livraison) et exp\u00e9di\u00e9e (BC \u2192 BL), en jours ouvr\u00e9s.\n[COMMENT CONSTRUITE] Lignes d'articles de vente de stg_mssql_sage__f_docligne (BC, PL, BL, factures ; m\u00eames filtres que app_cockpit__nunshen_bl_client) regroup\u00e9es par n\u00b0 de BC : la ligne transform\u00e9e recopie n\u00b0 et date du BC, de la PL et du BL ; une pi\u00e8ce ouverte porte son propre n\u00b0 et sa date. Premi\u00e8re date de chaque \u00e9tape. BC annul\u00e9s = statut 1 dans stg_mssql_sage__f_docentete.\n[GRAIN] 1 ligne par bon de commande (commande_numero), BC dat\u00e9s depuis le 01/01/2024.\n[NOTES] Sage ne garde que le jour (pas l'heure) et la pr\u00e9paration valid\u00e9e est le BL lui-m\u00eame. Pi\u00e8ces sans BC (devis transform\u00e9 en pr\u00e9paration, BL direct) absentes. Les d\u00e9lais, leurs seuils et la conformit\u00e9 sont calcul\u00e9s par l'app avec app_cockpit__general_calendrier_ouvre. R\u00e9serv\u00e9 \u00e0 l'application : pas de rapport Power BI dessus.\n"""
    )
    as (
      

-- Commandes clients Nunshen (Sage) et date de leurs étapes, pour les délais de traitement des commandes
-- (ISO) de Cockpit Supply. Les délais et leur conformité sont calculés par l'app. Sage ne garde que le JOUR
-- de chaque étape : à la transformation, la ligne recopie le n° et la date du bon de commande (BC), de la
-- préparation de livraison (PL) et du BL. Une pièce encore ouverte porte son propre n° et sa propre date.
-- Préparation lancée = PL ; préparée et expédiée = BL (même document dans Sage).
with lignes as (
    select
        coalesce(dl_piece_bc, if(do_type = 1, do_piece, null)) as n_piece_bc,
        if(dl_piece_bc is not null, date(dl_date_bc), if(do_type = 1, date(do_date), null)) as date_bc,
        if(dl_piece_pl is not null, date(dl_date_pl), if(do_type = 2, date(do_date), null)) as date_pl,
        coalesce(dl_piece_bl, if(do_type = 3, do_piece, null)) as n_piece_bl,
        if(dl_piece_bl is not null, date(dl_date_bl), if(do_type = 3, date(do_date), null)) as date_bl,
        do_type in (6, 7) and starts_with(do_piece, 'ZZ') as is_intragroupe,
        extracted_at
    from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_docligne`
    -- Lignes d'articles de vente (BC, PL, BL, factures), mêmes filtres que app_cockpit__nunshen_bl_client
    where
        do_domaine = 0
        and do_type in (1, 2, 3, 6, 7)
        and ar_ref is not null
        and dl_valorise = 1
        and dl_mvt_stock != 1
),

-- Statut 1 d'un bon de commande = « Annulé » dans le paramétrage Sage de Nunshen
bc_annules as (
    select do_piece
    from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_docentete`
    where
        do_domaine = 0
        and do_type = 1
        and do_statut = 1
),

commandes as (
    select
        n_piece_bc,
        min(date_bc) as commande_date,
        min(date_pl) as preparation_lancee_date,
        min(date_bl) as expedition_date,
        count(distinct n_piece_bl) as nb_bl,
        logical_or(is_intragroupe) as is_intragroupe,
        max(extracted_at) as extracted_at
    from lignes
    -- Pièce sans bon de commande (devis transformé en préparation, BL direct) : pas de date de commande
    where n_piece_bc is not null
    group by n_piece_bc
)

select
    c.n_piece_bc as commande_numero,
    'METROPOLE' as calendrier,
    c.commande_date,
    c.preparation_lancee_date,
    c.expedition_date,
    c.nb_bl,
    c.is_intragroupe,
    if(a.do_piece is not null, 'ANNULEE', null) as motif_exclusion,
    c.extracted_at
from commandes as c
left join bc_annules as a
    on c.n_piece_bc = a.do_piece
where c.commande_date >= date('2024-01-01')
    );
  