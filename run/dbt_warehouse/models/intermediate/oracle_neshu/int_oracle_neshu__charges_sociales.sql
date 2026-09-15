
  
    

    create or replace table `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__charges_sociales`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Enveloppe mensuelle de charges sociales saisie dans l'ERP, \u00e0 r\u00e9partir entre les clients au prorata du temps pass\u00e9.\n[COMMENT CONSTRUITE] T\u00e2ches CHARGES_SOCIALES (type 242), statuts 0/1/2/4, montant extrait de la zone XML au noeud /ZONE/COUTRM par la macro extraire_balise_xml.\n[GRAIN] 1 ligne par mois.\n[NOTES] La saisie est MANUELLE, une t\u00e2che par mois. Au 2026-09-15 rien n'est saisi depuis juin 2026 : juillet et ao\u00fbt n'ont donc aucune ligne ici, et le co\u00fbt de main-d'oeuvre de ces mois vaut z\u00e9ro dans le P&L. C'est une donn\u00e9e absente \u00e0 la source, pas une erreur de calcul \u2014 le rapport Distrilog porte le m\u00eame trou. \u00c0 remonter \u00e0 la finance. La colonne nb_saisies expose une \u00e9ventuelle double saisie, que le max du mois masquerait sinon.\n"""
    )
    as (
      

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
        regexp_extract(t.xml, r'<COUTRM>([^<]*)</COUTRM>') as charges_texte,
        t.idtask,
        t.updated_at,
        t.extracted_at
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task` as t
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
    );
  