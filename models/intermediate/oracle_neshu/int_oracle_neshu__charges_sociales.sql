{{
    config(
        materialized='table',
        description='Enveloppe mensuelle de charges sociales saisie dans l''ERP, base du coût de main-d''oeuvre réparti'
    )
}}

-- Une tâche CHARGES_SOCIALES par mois, dont le montant est rangé dans la zone
-- XML libre au noeud /ZONE/COUTRM. Le rapport Distrilog prend le MAX du mois :
-- on reproduit, ce qui protège d'une double saisie sans la masquer (nb_saisies
-- l'expose).
--
-- ⚠️ La saisie est MANUELLE et peut manquer : au 2026-09-15, rien n'est saisi
-- depuis juin 2026. Un mois sans saisie ne produit aucune ligne ici, et le coût
-- de main-d'oeuvre de ce mois vaudra donc zéro dans le P&L — pas une erreur de
-- calcul, une donnée absente à la source.

with source_data as (

    select
        date_trunc(date(t.real_start_date), month) as mois,
        {{ extraire_balise_xml('t.xml', 'COUTRM') }} as charges_texte,
        t.idtask,
        t.updated_at,
        t.extracted_at
    from {{ ref('stg_oracle_neshu__task') }} as t
    where
        t.idtask_type = 242  -- CHARGES_SOCIALES
        and t.code_status_record = '1'
        and t.idtask_status in (0, 1, 2, 4)  -- PREVU, FAIT, ENCOURS, VALIDE
        and t.real_start_date is not null
),

charges_mensuelles as (

    select
        mois,
        max(safe_cast(charges_texte as float64)) as charges_sociales_eur,
        count(distinct idtask) as nb_saisies,
        max(updated_at) as updated_at,
        max(extracted_at) as extracted_at
    from source_data
    where charges_texte is not null
    group by mois
)

select
    mois,
    charges_sociales_eur,
    nb_saisies,
    updated_at,
    extracted_at
from charges_mensuelles
