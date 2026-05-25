
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`fct_commerce__opportunite`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER]\nFaits des opportunit\u00e9s commerciales Nespresso : pipeline pr\u00e9-vente avec\nstatut, dates, valeur nette attendue, probabilit\u00e9 de succ\u00e8s, commercial\nresponsable, campagne marketing et indicateur premi\u00e8re commande caf\u00e9.\n\n[COMMENT CONSTRUITE]\nConstruit \u00e0 partir de `stg_nesp_co__opportunite` (fichier Excel\n`nespresso_opportunites`). Renommage des colonnes en pr\u00e9fixe `opp_*`,\ntraduction en fran\u00e7ais des statuts (`Won`\u2192`Gagn\u00e9`, `Lost`\u2192`Perdu`,\n`Open`/`In Process`\u2192`En cours`) et des r\u00f4les (`Customer`\u2192`Client`,\n`Prospect`\u2192`Client potentiel`), conversion de la probabilit\u00e9 en\nratio 0-1, et nettoyage des ID C4C (`#`\u2192NULL).\n\n[GRAIN]\n1 ligne par opportunit\u00e9 (`opp_id`).\n\n[NOTES]\nSource : fichier Excel daily nesp_co (rafra\u00eechi 08:00 weekdays). Lien\nclient : `opp_id_compte` (format `FR_<nessoft>`) ou `opp_id_client_c4c`\n(ID C4C). Pas de FK directe vers `dim_commerce__client` : la jointure\nse fait c\u00f4t\u00e9 BI ou via un mod\u00e8le d\u00e9di\u00e9 (cf.\n`fct_commerce__machine_intervention` pour le pattern d'extraction du\ntiers depuis le code Nessoft via regex).\n"""
    )
    as (
      

select * from `evs-datastack-prod`.`prod_intermediate`.`int_nesp_co__opportunites`
    );
  