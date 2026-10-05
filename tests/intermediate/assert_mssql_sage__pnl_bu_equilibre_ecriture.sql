{{ config(severity='error', warn_if='>60', error_if='>200') }}

-- Invariant comptable : pour chaque écriture ventilée, la somme des lignes
-- analytiques signées doit redonner le montant signé de l'écriture générale.
--
-- Garde-fou du correctif de signe (2026-10-05) : montant_analytique_signe
-- appliquait abs() avant le sens, ce qui inversait les montants analytiques
-- négatifs (réaffectations entre sections). Aucune des 382 écritures concernées
-- ne s'équilibrait ; ~1 M€ de résultat 2025 manquait au P&L BU.
--
-- Seuils et non zéro : 43 écritures sont déséquilibrées DANS SAGE (ventilation
-- analytique saisie différente du montant général, ex. 360 € ventilés 32 856 €),
-- indépendamment de la règle de signe. Relevé le 2026-10-05 : 43.
-- warn au-delà de 60 (nouvelles saisies à remonter à la compta),
-- error au-delà de 200 (régression de la règle de signe : ~420 avec abs()).

with ecritures_generales as (
    select
        ec_no as numero_ecriture_comptable,
        case when ec_sens = 0 then -ec_montant else ec_montant end as montant_general_signe
    from {{ ref('stg_mssql_sage__f_ecriturec') }}
),

ventilation as (
    select
        numero_ecriture_comptable,
        numero_plan_analytique,
        sum(montant_analytique_signe) as montant_analytique_signe
    from {{ ref('int_mssql_sage__pnl_bu') }}
    where not is_missing_analytical
    group by numero_ecriture_comptable, numero_plan_analytique
)

select
    v.numero_ecriture_comptable,
    v.numero_plan_analytique,
    g.montant_general_signe,
    v.montant_analytique_signe,
    v.montant_analytique_signe - g.montant_general_signe as ecart
from ventilation as v
inner join ecritures_generales as g
    on v.numero_ecriture_comptable = g.numero_ecriture_comptable
where abs(v.montant_analytique_signe - g.montant_general_signe) >= 0.02
