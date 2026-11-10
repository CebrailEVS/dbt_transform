{{ config(
    materialized='table',
    partition_by={'field': 'date_mouvement', 'data_type': 'date', 'granularity': 'month'},
    cluster_by=['depot_id', 'reference']
) }}

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
    from {{ ref('stg_mssql_sage__f_docligne') }} as dl
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
    from {{ ref('stg_mssql_sage__f_docligne') }}
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
left join {{ ref('stg_mssql_sage__f_depot') }} as dep
    on l.de_no = dep.de_no
left join {{ ref('stg_mssql_sage__f_article') }} as ar
    on l.ar_ref = ar.ar_ref
left join transferts as t
    on l.do_type = 23 and l.do_piece = t.do_piece
left join {{ ref('stg_mssql_sage__f_depot') }} as src
    on t.depot_source_id = src.de_no
left join {{ ref('stg_mssql_sage__f_depot') }} as dst
    on t.depot_destination_id = dst.de_no
