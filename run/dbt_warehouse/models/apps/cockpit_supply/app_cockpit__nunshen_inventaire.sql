
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__nunshen_inventaire`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Sessions d'inventaire Nunshen (date \u00d7 d\u00e9p\u00f4t) : indicateur ISO \u00ab fr\u00e9quence des inventaires \u00bb.\n[COMMENT CONSTRUITE] Pi\u00e8ces d'inventaire \u00ab i\u2026 \u00bb d'entr\u00e9e et de sortie (do_domaine 2, types 20 et 21) de stg_mssql_sage__f_docligne, regroup\u00e9es par date et d\u00e9p\u00f4t (ligne, sinon en-t\u00eate) : nombre de pi\u00e8ces et de r\u00e9f\u00e9rences, quantit\u00e9s et valeurs ajust\u00e9es.\n[GRAIN] 1 ligne par (date_inventaire, depot_id).\n[NOTES] Remplace le proxy \u00ab nombre d'imports de stock \u00bb de l'ISO. Sage n'\u00e9crit une pi\u00e8ce que s'il y a un \u00e9cart : un inventaire sans \u00e9cart n'y figure pas (d\u00e9finition \u00e0 valider par le pilote ISO). R\u00e9serv\u00e9 \u00e0 l'application : pas de rapport Power BI dessus.\n"""
    )
    as (
      

-- Sessions d'inventaire Nunshen (Sage, pièces « i… » d'entrée et de sortie, types 20/21)
-- pour Cockpit Supply : remplace le proxy « nombre d'imports de stock » de l'ISO
-- (fréquence des inventaires). Une session = une date × un dépôt. Sage n'écrit une pièce
-- d'inventaire que s'il y a un écart : un inventaire sans écart n'y figure pas.
select
    date(dl.do_date) as date_inventaire,
    coalesce(nullif(dl.de_no, 0), de.de_no) as depot_id,
    dep.de_intitule as depot,
    count(distinct dl.do_piece) as nb_pieces,
    count(distinct dl.ar_ref) as nb_references,
    cast(sum(if(dl.do_type = 20, dl.dl_qte, 0)) as float64) as qte_entree,
    cast(sum(if(dl.do_type = 21, dl.dl_qte, 0)) as float64) as qte_sortie,
    cast(sum(if(dl.do_type = 20, dl.dl_montant_ht, 0)) as float64) as valeur_entree,
    cast(sum(if(dl.do_type = 21, dl.dl_montant_ht, 0)) as float64) as valeur_sortie,
    max(dl.extracted_at) as extracted_at
from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_docligne` as dl
left join `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_docentete` as de
    on dl.do_type = de.do_type and dl.do_piece = de.do_piece
left join `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_depot` as dep
    on coalesce(nullif(dl.de_no, 0), de.de_no) = dep.de_no
where
    dl.do_domaine = 2
    and dl.do_type in (20, 21)
    and starts_with(lower(dl.do_piece), 'i')
group by date_inventaire, depot_id, depot
    );
  