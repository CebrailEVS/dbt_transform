
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__technique_stock_van`
      
    partition by date_trunc(stock_date, month)
    cluster by stock

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Stock quotidien des vans des techniciens TechCare, pi\u00e8ces pr\u00e9sentes seulement. Sert \u00e0 Cockpit Supply pour savoir, jour par jour, combien de vans actifs d'un d\u00e9p\u00f4t d\u00e9tiennent une pi\u00e8ce en rupture au d\u00e9p\u00f4t (taux de service historis\u00e9).\n[COMMENT CONSTRUITE] Lignes de fct_supply_chain__stock_yuman dont la zone commence par \u00ab ST - \u00bb et dont la quantit\u00e9 est positive (filtre ligne \u00e0 ligne), additionn\u00e9es par (stock_date, stock, reference).\n[GRAIN] 1 ligne par (stock_date, stock, reference).\n[NOTES] La correspondance van \u2192 d\u00e9p\u00f4t et le choix des vans actifs restent dans l'app (r\u00e9f\u00e9rentiel des techniciens) : elle les passe en param\u00e8tres d'une requ\u00eate qui regroupe par d\u00e9p\u00f4t, fournisseur et article. Partition mensuelle sur stock_date. R\u00e9serv\u00e9 \u00e0 l'application.\n"""
    )
    as (
      

-- Stock quotidien des vans des techniciens (zones « ST - NOM ») : lignes à quantité
-- positive, additionnées par jour, van et référence. L'app en tire par requête la
-- présence d'une pièce dans les vans actifs de chaque dépôt, jour par jour (taux de
-- service historisé), sans charger l'historique quotidien en mémoire.
select
    stock_date,
    stock,
    reference,
    sum(quantite) as quantite
from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__stock_yuman`
where
    starts_with(stock, 'ST - ')
    -- filtre ligne à ligne AVANT la somme, comme l'app
    and quantite > 0
group by stock_date, stock, reference
    );
  