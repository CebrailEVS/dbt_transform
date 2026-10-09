
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__nunshen_article`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Base article Nunshen : statut actif / en sommeil, famille et attributs qui retirent un article des analyses de disponibilit\u00e9 (signature exclusivit\u00e9 client, exclusivit\u00e9 boutique, produit sur demande).\n[COMMENT CONSTRUITE] stg_mssql_sage__f_article joint \u00e0 stg_mssql_sage__f_famille. actif = ar_sommeil = 0 ; champs libres OUI/NON convertis en bool\u00e9ens (vide = faux).\n[GRAIN] 1 ligne par r\u00e9f\u00e9rence (reference).\n[NOTES] Noms de colonnes repris tels que l'app les lit. R\u00e9serv\u00e9 \u00e0 l'application : pas de rapport Power BI dessus.\n"""
    )
    as (
      

-- Articles Nunshen (Sage) pour Cockpit Supply.
-- Colonnes nommées comme les attributs lus par l'app. Champs libres Sage OUI/NON → booléens.
select
    ar.ar_ref as reference,
    ar.ar_design as designation,
    ar.ar_sommeil = 0 as actif,
    ar.fa_code_famille as code_famille,
    fa.fa_intitule as libelle_famille,
    coalesce(ar.signature_exclusivite_client = 'OUI', false) as signature_exclusivite_client,
    coalesce(ar.exclusivite_boutique = 'OUI', false) as exclusivite_boutique,
    coalesce(ar.produit_sur_demande = 'OUI', false) as produit_sur_demande,
    ar.extracted_at
from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_article` as ar
left join `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_famille` as fa
    on ar.fa_code_famille = fa.fa_code_famille
    );
  