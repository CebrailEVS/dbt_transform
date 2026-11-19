
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__technique_mouvement_stock`
      
    
    cluster by journee

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Journal des mouvements de stock TechCare (Yuman), tel que lu par l'application Cockpit Supply : r\u00e9ceptions de bons de commande, transferts atelier \u21c4 v\u00e9hicule, poses en intervention, ajustements d'inventaire.\n[COMMENT CONSTRUITE] stg_yuman_evs_web__mouvements_stock r\u00e9duit aux colonnes lues par l'app, sans filtre de date ni de ligne. Colonnes renomm\u00e9es comme la table d'import de l'app qu'il remplace (import manuel \u00ab Logs de produits \u00bb) ; date de l'op\u00e9ration en heure de Paris ; rang du mouvement dans sa journ\u00e9e calcul\u00e9 depuis le rang de la ligne dans l'export.\n[GRAIN] 1 ligne par mouvement, identifi\u00e9 par (`journee`, `rang_journee`). Deux lignes identiques sont deux mouvements.\n[NOTES] Ordre des mouvements : `journee`, puis `rang_journee` (la date de l'op\u00e9ration est \u00e0 la minute). \u00c9crans : flux et cl\u00f4ture TechCare, kits pr\u00e9ventifs, sorties anormales, test du stock cible, consommation, taux de service et d\u00e9lais fournisseurs. Les r\u00e8gles de l'app (consommation, ajustements, poses Nomad, astreinte) restent appliqu\u00e9es par l'app. R\u00e9serv\u00e9 \u00e0 l'application, pas de rapport Power BI dessus.\n"""
    )
    as (
      

-- Journal des mouvements de stock TechCare (Yuman), pour Cockpit Supply : remplace l'import manuel
-- « Logs de produits ». Noms de colonnes de la table d'import de l'app ; aucune règle métier, l'app
-- applique les siennes (consommation, ajustements, poses Nomad, astreinte).
select
    journee,
    -- `_ligne` glisse avec la fenêtre d'export ; le rang dans la journée reste le même d'un
    -- chargement à l'autre et donne à l'app une clé de mouvement stable.
    row_number() over (partition by journee order by _ligne) as rang_journee,
    timestamp(datetime(operation_at, 'Europe/Paris')) as date_operation,
    reference,
    article,
    quantite as qte,
    prix_d_achat_unitaire as prix_achat_unitaire,
    stock_de_depart as stock_depart,
    quantite_stock_de_depart as qte_stock_depart,
    stock_de_destination as stock_destination,
    quantite_stock_de_destination as qte_stock_destination,
    utilisateur,
    observation,
    extracted_at
from `evs-datastack-prod`.`prod_staging`.`stg_yuman_evs_web__mouvements_stock`
    );
  