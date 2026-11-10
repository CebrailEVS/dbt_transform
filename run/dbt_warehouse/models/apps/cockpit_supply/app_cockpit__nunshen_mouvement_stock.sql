
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__nunshen_mouvement_stock`
      
    partition by date_trunc(date_mouvement, month)
    cluster by depot_id, reference

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Journal des mouvements de stock Nunshen, tous d\u00e9p\u00f4ts : r\u00e9ceptions, livraisons clients et avoirs, mouvements d'entr\u00e9e et de sortie (r\u00e9gularisations, inventaires, d\u00e9conditionnement), transferts entre d\u00e9p\u00f4ts, bons de fabrication complets (produit fini, composants consomm\u00e9s, prestations de fa\u00e7onnage). Explique le passage d'une photo de stock \u00e0 la suivante : flux du mois et cl\u00f4ture mensuelle.\n[COMMENT CONSTRUITE] Lignes de stg_mssql_sage__f_docligne qui bougent le stock (dl_mvt_stock 1 = entr\u00e9e, 3 = sortie) et toutes les lignes des bons de fabrication (type 26), y compris les prestations sans mouvement. Quantit\u00e9 et valeur sign\u00e9es (+ entr\u00e9e, \u2212 sortie) : sens donn\u00e9 par dl_mvt_stock, quantit\u00e9 par abs(dl_qte), car un avoir est not\u00e9 en quantit\u00e9 n\u00e9gative avec dl_mvt_stock = 1. Valeur : entr\u00e9e au prix de revient (dl_prix_ru), sortie au CMUP (dl_cmup), comme Sage valorise son stock. D\u00e9p\u00f4ts source et destination repris sur chaque ligne d'un transfert.\n[GRAIN] 1 ligne par ligne de document Sage (ligne_id), mouvements dat\u00e9s depuis janvier 2024 (filtre du mod\u00e8le).\n[NOTES] Date du mouvement = date du BL (une ligne de BL factur\u00e9e garde la date du BL, pas celle de la facture) ; mouvement_at = moment o\u00f9 le mouvement est pass\u00e9 dans le stock, \u00e0 comparer \u00e0 l'heure d'une photo de stock (une ligne de document transform\u00e9, commande \u2192 r\u00e9ception par exemple, garde la saisie de la ligne d'origine). Sage ne garde que l'\u00e9tat courant d'un document : une ligne supprim\u00e9e dispara\u00eet, une ligne modifi\u00e9e garde sa date de saisie (modifie_at la signale) ; des \u00e9carts d'un jour \u00e0 l'autre avec les photos sont donc possibles, ils se compensent quand la modification reste dans le mois. Une revalorisation du CMUP sans document n'y figure pas (effet prix). R\u00e9serv\u00e9 \u00e0 l'application : pas de rapport Power BI dessus.\n"""
    )
    as (
      

-- Mouvements de stock Nunshen (Sage) pour Cockpit Supply : toute ligne de document qui fait bouger
-- le stock (dl_mvt_stock 1 = entrée, 3 = sortie), plus les lignes de bons de fabrication sans
-- mouvement (prestations de façonnage), pour avoir chaque bon complet.
-- Sens donné par dl_mvt_stock, quantité par abs(dl_qte) : Sage note un avoir en quantité négative
-- avec dl_mvt_stock = 1, et il fait bien entrer la marchandise.
-- Valeur au coût de stock Sage : entrée au prix de revient (dl_prix_ru), sortie au CMUP (dl_cmup).
-- created_at Sage est en heure de Paris (horloge du serveur) : converti en UTC. Une ligne de document
-- transformé (commande → réception, préparation → bon de fabrication, BL → facture) garde la date de
-- saisie de la ligne d'origine : le mouvement n'a lieu qu'à la date de la pièce (mouvement_at).
with lignes as (
    select
        dl.*,
        case dl.dl_mvt_stock when 1 then 'entree' when 3 then 'sortie' else 'aucun' end as sens
    from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_docligne` as dl
    where
        dl.ar_ref is not null
        and (dl.dl_mvt_stock in (1, 3) or dl.do_type = 26)
        and coalesce(dl.dl_date_bl, dl.do_date) >= timestamp('2024-01-01')
),

-- Un transfert (virement de dépôt à dépôt) a, par article, une ligne de sortie au dépôt source
-- suivie d'une ligne d'entrée au dépôt destination ; un seul couple de dépôts par pièce.
transferts as (
    select
        do_piece,
        max(if(dl_mvt_stock = 3, de_no, null)) as depot_source_id,
        max(if(dl_mvt_stock = 1, de_no, null)) as depot_destination_id
    from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_docligne`
    where do_type = 23
    group by do_piece
)

select
    l.dl_no as ligne_id,
    l.do_type as type_document,
    case l.do_type
        when 3 then 'Bon de livraison'
        when 4 then 'Bon de retour'
        when 6 then 'Facture'
        when 7 then 'Facture comptabilisée'
        when 13 then 'Bon de réception'
        when 20 then 'Entrée de stock'
        when 21 then 'Sortie de stock'
        when 23 then 'Transfert entre dépôts'
        when 26 then 'Bon de fabrication'
        else concat('Type Sage ', cast(l.do_type as string))
    end as libelle_type_document,
    l.do_piece as n_piece,
    l.dl_ligne as n_ligne,
    l.do_ref as reference_document,
    date(l.do_date) as date_document,
    date(coalesce(l.dl_date_bl, l.do_date)) as date_mouvement,
    timestamp(datetime(l.created_at), 'Europe/Paris') as saisie_at,
    greatest(
        timestamp(datetime(l.created_at), 'Europe/Paris'),
        timestamp(date(coalesce(l.dl_date_bl, l.do_date)), 'Europe/Paris')
    ) as mouvement_at,
    timestamp(datetime(l.updated_at), 'Europe/Paris') as modifie_at,
    nullif(l.de_no, 0) as depot_id,
    dep.de_intitule as depot,
    l.ar_ref as reference,
    coalesce(ar.ar_design, l.dl_design) as designation,
    ar.fa_code_famille as code_famille,
    if(l.do_type = 26, l.ar_ref_compose, null) as reference_compose,
    l.sens,
    cast(abs(l.dl_qte) as float64) as quantite_document,
    cast(case l.sens when 'entree' then abs(l.dl_qte) when 'sortie' then -abs(l.dl_qte) else 0 end as float64) as quantite,
    cast(if(l.sens = 'sortie', l.dl_cmup, l.dl_prix_ru) as float64) as prix_unitaire_stock,
    cast(case l.sens
        when 'entree' then abs(l.dl_qte) * l.dl_prix_ru
        when 'sortie' then -abs(l.dl_qte) * l.dl_cmup
        else 0
    end as float64) as valeur_stock,
    cast(l.dl_montant_ht as float64) as montant_ht,
    if(l.do_domaine in (0, 1), l.ct_num, null) as code_tiers,
    t.depot_source_id,
    src.de_intitule as depot_source,
    t.depot_destination_id,
    dst.de_intitule as depot_destination,
    l.extracted_at
from lignes as l
left join `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_depot` as dep
    on l.de_no = dep.de_no
left join `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_article` as ar
    on l.ar_ref = ar.ar_ref
left join transferts as t
    on l.do_type = 23 and l.do_piece = t.do_piece
left join `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_depot` as src
    on t.depot_source_id = src.de_no
left join `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_depot` as dst
    on t.depot_destination_id = dst.de_no
    );
  