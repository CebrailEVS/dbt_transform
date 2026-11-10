{{ config(severity='warn') }}
-- N° de facture e-shop hors format (3 à 6 chiffres, sans zéro de tête) : il ne se rapprocherait d'aucune
-- facture de l'export WooCommerce, ramenée aux chiffres par l'app.
select
    numero_facture_eshop,
    count(*) as nb_lignes
from {{ ref('app_cockpit__lcdp_livraison_interne') }}
where
    numero_facture_eshop is not null
    and not regexp_contains(numero_facture_eshop, r'^[1-9][0-9]{2,5}$')
group by numero_facture_eshop
