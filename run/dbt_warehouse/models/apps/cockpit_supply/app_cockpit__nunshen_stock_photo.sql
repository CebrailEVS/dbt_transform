
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__nunshen_stock_photo`
      
    partition by date_trunc(stock_date, month)
    cluster by depot_id, reference

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Stock Nunshen par d\u00e9p\u00f4t et r\u00e9f\u00e9rence, en quantit\u00e9 et en valeur (CMUP) : stock disponible (d\u00e9p\u00f4t NUNSHEN), stock valoris\u00e9 tous d\u00e9p\u00f4ts, photos de fin de mois du flux et du CODIR.\n[COMMENT CONSTRUITE] stg_mssql_sage__f_artstock (photos compl\u00e8tes \u00e0 chaque extraction) : une photo par jour, la derni\u00e8re extraction du jour en heure de Paris ; gard\u00e9es : la photo courante et la derni\u00e8re photo de chaque mois. Lignes \u00e0 quantit\u00e9 et valeur nulles retir\u00e9es. D\u00e9p\u00f4t, d\u00e9signation et famille joints.\n[GRAIN] 1 ligne par (stock_date, depot_id, reference).\n[NOTES] Remplace les imports \u00ab Stock Wissous \u00bb (quantit\u00e9s, totaux par r\u00e9f\u00e9rence) et \u00ab Valo stock \u00bb (valeur). Historique depuis le 2026-09-23 : les relev\u00e9s ant\u00e9rieurs restent dans Supabase, lus par l'app pour les mois pass\u00e9s. R\u00e9serv\u00e9 \u00e0 l'application : pas de rapport Power BI dessus.\n"""
    )
    as (
      

-- Stock Nunshen par dépôt et référence (Sage, photos quotidiennes de f_artstock) pour
-- Cockpit Supply : remplace les imports « Stock Wissous » (quantités) et « Valo stock »
-- (valeur, CMUP). Une photo par jour (la dernière extraction du jour, heure de Paris) ;
-- gardées : la photo courante et la dernière photo de chaque mois (flux et CODIR).
-- Stock disponible de l'app = dépôt NUNSHEN (depot_id 1) ; valeur de stock = tous dépôts.
with extractions as (
    select distinct extracted_at
    from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_artstock`
),
photos as (
    select
        extracted_at,
        date(extracted_at, 'Europe/Paris') as stock_date
    from extractions
    qualify row_number() over (partition by date(extracted_at, 'Europe/Paris') order by extracted_at desc) = 1
),
reperes as (
    select
        *,
        stock_date = max(stock_date) over (partition by date_trunc(stock_date, month)) as is_derniere_photo_mois,
        stock_date = max(stock_date) over () as is_photo_courante
    from photos
)
select
    r.stock_date,
    st.de_no as depot_id,
    dep.de_intitule as depot,
    st.ar_ref as reference,
    ar.ar_design as designation,
    ar.fa_code_famille as code_famille,
    cast(st.as_qte_sto as float64) as stock_reel,
    cast(st.as_mont_sto as float64) as valorisation,
    cast(safe_divide(st.as_mont_sto, st.as_qte_sto) as float64) as prix_unitaire,
    r.is_derniere_photo_mois,
    r.is_photo_courante,
    st.extracted_at
from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_artstock` as st
inner join reperes as r
    on st.extracted_at = r.extracted_at
left join `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_depot` as dep
    on st.de_no = dep.de_no
left join `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_article` as ar
    on st.ar_ref = ar.ar_ref
where
    (r.is_derniere_photo_mois or r.is_photo_courante)
    and (st.as_qte_sto != 0 or st.as_mont_sto != 0)
    );
  