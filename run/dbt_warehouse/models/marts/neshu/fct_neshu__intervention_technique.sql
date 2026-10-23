
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`fct_neshu__intervention_technique`
      
    
    cluster by device_id, company_id, resources_id

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Interventions techniques NESHU sur les distributeurs : entretien (d\u00e9tartrage, lavage sanitaire) et d\u00e9pannage de 1er niveau, faits par les roadmen pendant leur tourn\u00e9e, et plus rarement par les GEA. Une ligne par intervention, avec la machine, le client, l'intervenant, la classification (objet, d\u00e9tail), le statut de traitement et le diagnostic saisi.\n[COMMENT CONSTRUITE] Fait mince sur int_oracle_neshu__inter_technique_tasks (t\u00e2ches de type 131, statuts FAIT / VALIDE / ANNULE / ANOMALIE), qui porte les labels en code + libell\u00e9 fr_FR. Ajoute les noms d'affichage depuis dim_neshu__device, dim_neshu__company et dim_neshu__resource : code roadman, code GEA et agence (d\u00e9p\u00f4t de rattachement de l'intervenant). FK vers le mod\u00e8le de la machine (dim_neshu__modele_equipement) et vers la pi\u00e8ce (dim_neshu__piece). Instants en heure de Paris.\n[GRAIN] 1 ligne par task_id (intervention). Environ 3 200 lignes en octobre 2026, +450 par mois.\n[NOTES] Processus actif en continu depuis mars 2026 (400 \u00e0 500 interventions par mois) ; avant, des t\u00e2ches ponctuelles (dont 104 en juillet 2025). Classification \u00e0 deux niveaux : objet_intervent (C1 \u00e0 C8) puis objet_intervent_detail, \u00e0 lire ensemble (un code de d\u00e9tail n'est pas unique d'une famille \u00e0 l'autre). C4 (OC04) = entretien, r\u00e9solu sur place \u00e0 98 % ; les autres objets rel\u00e8vent du d\u00e9pannage, transmis \u00e0 la technique (TOTECH) dans environ 80 % des cas. company_id est le client au moment de l'intervention, qui peut diff\u00e9rer du client actuel de la machine. roadman_code, gea_code et agence d\u00e9crivent l'intervenant aujourd'hui (non historis\u00e9s). task_end_date suit task_start_date de quelques secondes \u00e0 quelques minutes, avec des valeurs aberrantes : aucune dur\u00e9e d'intervention exploitable.\n"""
    )
    as (
      

with interventions as (
    select * from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__inter_technique_tasks`
),

devices as (
    select
        device_id,
        device_name
    from `evs-datastack-prod`.`prod_marts`.`dim_neshu__device`
),

companies as (
    select
        company_id,
        company_name
    from `evs-datastack-prod`.`prod_marts`.`dim_neshu__company`
),

resources as (
    select
        resources_id,
        resources_code,
        gea_code,
        company_storehouse_id
    from `evs-datastack-prod`.`prod_marts`.`dim_neshu__resource`
)

select
    -- Grain
    date(i.task_start_date, 'Europe/Paris') as intervention_date,
    i.task_id,

    -- FK
    i.device_id,
    i.modele_equipement_id,
    i.company_id,
    i.resources_id,
    i.product_id as piece_id,

    -- Attributs d'affichage
    i.document_number,
    i.device_code,
    d.device_name,
    i.company_code,
    c.company_name,
    r.resources_code as roadman_code,
    r.gea_code,
    -- Agence = dépôt de rattachement actuel de l'intervenant, pas celui du client.
    a.company_name as agence,
    i.product_code as piece_code,

    -- Statut et labels : code + libellé fr_FR
    i.task_status_code,
    i.statut_inter,
    i.statut_inter_label,
    i.objet_intervent,
    i.objet_intervent_label,
    i.objet_intervent_detail,
    i.objet_intervent_detail_label,
    i.affichage,
    i.affichage_label,
    i.facturation,
    i.facturation_label,

    -- Commentaires
    i.comments_peer,
    i.comments_self,

    -- Dates (heure de Paris)
    timestamp(datetime(i.task_start_date, 'Europe/Paris')) as task_start_date,
    timestamp(datetime(i.task_end_date, 'Europe/Paris')) as task_end_date,

    -- Métadonnées (heure de Paris)
    timestamp(datetime(i.created_at, 'Europe/Paris')) as created_at,
    timestamp(datetime(i.updated_at, 'Europe/Paris')) as updated_at

from interventions as i
left join devices as d
    on i.device_id = d.device_id
left join companies as c
    on i.company_id = c.company_id
left join resources as r
    on i.resources_id = r.resources_id
left join companies as a
    on r.company_storehouse_id = a.company_id
    );
  