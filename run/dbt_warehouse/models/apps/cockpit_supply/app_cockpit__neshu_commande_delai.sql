
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__neshu_commande_delai`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Commandes Neshu et horodatage de leurs \u00e9tapes, pour les indicateurs ISO de traitement des commandes : commandes internes des approvisionneurs (chargement des v\u00e9hicules) et commandes clients. D\u00e9lai de prise en charge (cr\u00e9ation \u2192 premi\u00e8re impression), pr\u00e9par\u00e9e dans les d\u00e9lais (cr\u00e9ation \u2192 fin de pr\u00e9paration), dur\u00e9e de pr\u00e9paration (impression \u2192 fin, attente dans la pile comprise), exp\u00e9di\u00e9e dans les d\u00e9lais (clients).\n[COMMENT CONSTRUITE] Commandes (types 100 client, 132 interne) de stg_oracle_neshu__task depuis septembre 2023. Impression = premi\u00e8re action PRINT (86) de int_oracle_neshu__task_historic_metier. Document suivant = cible de TRANSFORM_TO (111) de l'historique, \u00e0 d\u00e9faut le m\u00eame dossier (task_idtask) : mouvement de stock (161) pour une commande interne, BL (101) et facture (102) pour une commande client. Article stock\u00e9 = stg_oracle_neshu__product.stock_management = 1. Heures de Paris (utc_vers_paris_affichage).\n[GRAIN] 1 ligne par commande (commande_id), commandes cr\u00e9\u00e9es depuis le 01/09/2023 (historique complet depuis le 31/08/2023).\n[NOTES] Les d\u00e9lais, leurs seuils (app_config) et la conformit\u00e9 sont calcul\u00e9s par l'app avec app_cockpit__general_calendrier_ouvre. Une commande non imprim\u00e9e a imprime_le vide : exclue de la prise en charge et de la dur\u00e9e, gard\u00e9e dans \u00ab pr\u00e9par\u00e9e dans les d\u00e9lais \u00bb. motif_exclusion vide = commande dans le p\u00e9rim\u00e8tre. Aucun identifiant de personne : indicateurs d'\u00e9quipe seulement. R\u00e9serv\u00e9 \u00e0 l'application : pas de rapport Power BI dessus.\n"""
    )
    as (
      

-- Commandes Neshu et horodatage de leurs étapes, pour les délais de traitement des commandes (ISO) de
-- Cockpit Supply. Les délais et leur conformité sont calculés par l'app (délais de référence modifiables,
-- jours ouvrés de app_cockpit__general_calendrier_ouvre).
-- Interne = commande d'approvisionneur (132) ; fin de préparation = création du mouvement de stock (161).
-- Client = commande client (100) ; fin de préparation et expédition = création du BL (101).
-- Prise en charge = première impression de la commande (action PRINT de l'historique des tâches).
with taches as (
    select
        idtask,
        task_idtask,
        idtask_type,
        idtask_status,
        type_product_source,
        idproduct_source,
        type_product_destination,
        created_at
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task`
    where
        idtask_type in (100, 101, 102, 132, 161)
        and code_status_record = '1'
),

commandes as (
    select *
    from taches
    where
        idtask_type in (100, 132)
        -- L'historique des tâches n'est complet que depuis le 31/08/2023 : avant, l'impression est inconnue
        and created_at >= timestamp('2023-09-01', 'Europe/Paris')
),

historique as (
    select
        task_id,
        historic_action_id,
        task_source_id,
        created_at
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__task_historic_metier`
    where
        task_type_id in (100, 132)
        and historic_action_id in (86, 111) -- PRINT, TRANSFORM_TO
        and is_historique_complet
),

impressions as (
    select
        task_id as idtask,
        min(created_at) as imprime_le
    from historique
    where historic_action_id = 86
    group by task_id
),

-- Document créé à partir de la commande : cible de TRANSFORM_TO (task_source_id), à défaut un document
-- du même dossier de gestion commerciale (task_idtask)
suivants as (
    select
        c.idtask as idtask_commande,
        s.idtask_type,
        s.created_at
    from commandes as c
    inner join historique as h
        on
            c.idtask = h.task_id
            and h.historic_action_id = 111
    inner join taches as s
        on h.task_source_id = s.idtask

    union distinct

    select
        c.idtask as idtask_commande,
        s.idtask_type,
        s.created_at
    from commandes as c
    inner join taches as s
        on
            c.task_idtask = s.task_idtask
            and c.idtask != s.idtask
    where s.idtask_type in (101, 102, 161)
),

etapes as (
    select
        idtask_commande as idtask,
        min(if(idtask_type = 161, created_at, null)) as mouvement_cree_le,
        min(if(idtask_type = 101, created_at, null)) as bl_cree_le,
        min(if(idtask_type = 102, created_at, null)) as facture_cree_le
    from suivants
    group by idtask_commande
),

articles_stockes as (
    select distinct thp.idtask
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task_has_product` as thp
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__product` as p
        on thp.idproduct = p.idproduct
    where p.stock_management = 1
),

enrichies as (
    select
        c.idtask,
        c.idtask_type,
        c.created_at,
        ts.code as statut_code,
        if(c.type_product_source = 'COMPANY', co.code, null) as depot_code,
        c.type_product_source = 'COMPANY' and c.type_product_destination = 'RESOURCES' as is_depot_vers_vehicule,
        a.idtask is not null as has_article_stocke,
        i.imprime_le,
        e.mouvement_cree_le,
        e.bl_cree_le,
        e.facture_cree_le
    from commandes as c
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task_status` as ts
        on c.idtask_status = ts.idtask_status
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__company` as co
        on c.idproduct_source = co.idcompany
    left join articles_stockes as a
        on c.idtask = a.idtask
    left join impressions as i
        on c.idtask = i.idtask
    left join etapes as e
        on c.idtask = e.idtask
)

select
    idtask as commande_id,
    if(idtask_type = 132, 'INTERNE', 'CLIENT') as type_commande,
    depot_code,
    -- Seul le dépôt de Strasbourg suit le calendrier d'Alsace-Moselle (Vendredi saint, 26 décembre)
    if(depot_code = 'DEPOTSTRASBOURG', 'ALSACE_MOSELLE', 'METROPOLE') as calendrier,
    statut_code,
    timestamp(datetime(created_at, 'Europe/Paris')) as cree_le,
    timestamp(datetime(imprime_le, 'Europe/Paris')) as imprime_le,
    timestamp(datetime(if(idtask_type = 132, mouvement_cree_le, bl_cree_le), 'Europe/Paris')) as preparation_fin_le,
    timestamp(datetime(if(idtask_type = 100, bl_cree_le, null), 'Europe/Paris')) as expedie_le,
    timestamp(datetime(if(idtask_type = 100, facture_cree_le, null), 'Europe/Paris')) as facture_le,
    case
        when statut_code = 'ANNULE' then 'ANNULEE'
        -- Hors chargement de véhicule : transferts entre dépôts, retours de véhicule
        when idtask_type = 132 and not is_depot_vers_vehicule then 'HORS_CHARGEMENT_VEHICULE'
        -- Badges, crédits de badge : rien à préparer
        when idtask_type = 100 and not has_article_stocke then 'SANS_ARTICLE_STOCKE'
        -- Facturée sans BL alors qu'elle porte un article stocké : pas de date d'expédition
        when idtask_type = 100 and bl_cree_le is null and facture_cree_le is not null then 'FACTUREE_SANS_BL'
    end as motif_exclusion
from enrichies
    );
  