{{ config(materialized='table') }}

-- Commandes fournisseurs Nunshen ouvertes (Sage, type 12) pour Cockpit Supply : remplace
-- l'import « Commandes fournisseurs » (lignes « Bon de commande »). Chargement Sage en
-- remplacement : une ligne reçue disparaît de la commande, la quantité d'une ligne
-- ouverte est donc déjà le reste à livrer (qte_restante, lue par nun_qte_restante).
-- qte_livree = quantité déjà reçue sur la même commande et la même référence.
with receptions as (
    select
        dl_piece_bc,
        ar_ref,
        sum(dl_qte) as qte_livree
    from {{ ref('stg_mssql_sage__f_docligne') }}
    where do_type = 13 and dl_piece_bc is not null and ar_ref is not null
    group by dl_piece_bc, ar_ref
)
select
    'Bon de commande' as type_document,
    dl.do_piece as n_piece,
    dl.ar_ref as reference,
    coalesce(ar.ar_design, dl.dl_design) as designation,
    cast(dl.dl_qte as float64) as qte_commandee,
    cast(dl.dl_qte as float64) as qte_restante,
    cast(coalesce(r.qte_livree, 0) as float64) as qte_livree,
    cast(dl.dl_prix_unitaire as float64) as prix_unitaire_ht,
    date(dl.do_date) as date_piece_bc,
    date(coalesce(dl.do_date_livr, de.do_date_livr)) as date_livraison,
    dep.de_intitule as depot,
    trim(dl.ct_num) as code_fournisseur,
    trim(ct.ct_intitule) as intitule_fournisseur,
    dl.extracted_at
from {{ ref('stg_mssql_sage__f_docligne') }} as dl
left join {{ ref('stg_mssql_sage__f_docentete') }} as de
    on dl.do_type = de.do_type and dl.do_piece = de.do_piece
left join {{ ref('stg_mssql_sage__f_depot') }} as dep
    on coalesce(nullif(dl.de_no, 0), de.de_no) = dep.de_no
left join {{ ref('stg_mssql_sage__f_article') }} as ar
    on dl.ar_ref = ar.ar_ref
left join {{ ref('stg_mssql_sage__f_comptet') }} as ct
    on dl.ct_num = ct.ct_num
left join receptions as r
    on dl.do_piece = r.dl_piece_bc and dl.ar_ref = r.ar_ref
where dl.do_type = 12 and dl.ar_ref is not null
