

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