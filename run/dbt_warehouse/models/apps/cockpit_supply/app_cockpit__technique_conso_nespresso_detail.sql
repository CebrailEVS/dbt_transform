
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__technique_conso_nespresso_detail`
      
    partition by timestamp_trunc(date_heure_debut, month)
    cluster by product_reference

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Pi\u00e8ces Nespresso pos\u00e9es par les techniciens TechCare, ligne \u00e0 ligne.\n[COMMENT CONSTRUITE] int_nesp_tech__articles_dedup \u00d7 int_nesp_tech__interventions_dedup (agences EVS), technicien retrouv\u00e9 par son matricule Nomad dans stg_yuman__users ; interventions \u00ab termin\u00e9e sign\u00e9e \u00bb / \u00ab termin\u00e9e non sign\u00e9e \u00bb, date renseign\u00e9e, codes de main-d'\u0153uvre 0000001 et 0000100 exclus ; r\u00e9f\u00e9rence normalis\u00e9e en product_reference, code_article brut conserv\u00e9.\n[GRAIN] 1 ligne par pi\u00e8ce pos\u00e9e lors d'une intervention (celui de la source).\n[NOTES] Jamais charg\u00e9e en m\u00e9moire : l'app l'interroge \u00e0 la demande (d\u00e9tail d'un mois pour l'export du flux TechCare, conso jour par jour d'une r\u00e9f\u00e9rence sur 12 mois glissants). Partition mensuelle sur date_heure_debut, clustering product_reference. R\u00e9serv\u00e9 \u00e0 l'application.\n"""
    )
    as (
      

-- Pièces Nespresso posées, ligne à ligne, construites depuis l'intermediate nesp_tech et le staging Yuman :
-- jamais chargée en mémoire, interrogée à la demande par l'app (détail d'un mois pour l'export du flux TechCare,
-- conso jour par jour d'une référence sur 12 mois). Interventions réalisées des agences EVS, date renseignée,
-- codes de main-d'œuvre exclus.
with techniciens as (
    -- Technicien Yuman retrouvé par son matricule Nomad.
    select
        user_id,
        nomad_id
    from `evs-datastack-prod`.`prod_staging`.`stg_yuman__users`
    where
        nomad_id is not null
        and (user_type = 'technician' or (user_type = 'manager' and is_manager_as_technician = true))
)

select
    i.date_heure_debut,
    a.code_article,
    coalesce(upper(trim(concat('EVS_NESPRESSO_', upper(a.code_article)))), 'NONE') as product_reference,
    t.user_id as technician_id,
    a.nom_article,
    coalesce(a.quantite_article, 0) as qty_consommee
from `evs-datastack-prod`.`prod_intermediate`.`int_nesp_tech__articles_dedup` as a
inner join `evs-datastack-prod`.`prod_intermediate`.`int_nesp_tech__interventions_dedup` as i
    on a.n_planning = i.n_planning
left join techniciens as t
    on lower(a.n_tech) = lower(cast(t.nomad_id as string))
where
    i.agency in ('evs', 'evs paris', 'evs idf', 'evs paris 2')
    and i.etat_intervention in ('terminée signée', 'terminée non signée')
    and i.date_heure_debut is not null
    and coalesce(upper(trim(a.code_article)), 'NONE') not in ('0000001', '0000100')
    );
  