
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`fct_lcdp__appel_sav`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Appels SAV re\u00e7us par LCDP (demandes client : approvisionnement, technique, commande, commerce\u2026), avec leur cat\u00e9gorisation saisie dans l'ERP, le client, la machine et la personne \u00e0 qui l'appel est affect\u00e9.\n[COMMENT CONSTRUITE] Issu de int_oracle_lcdp__appel_sav_tasks (t\u00e2ches idtask_type 130, enregistrements actifs, labels cat\u00e9gorie FAPP01 / sous-cat\u00e9gorie SAPPxx / d\u00e9tail pivot\u00e9s avec leurs libell\u00e9s fr_FR). Code et nom de la ressource aplatis depuis dim_lcdp__resource. Aucune cat\u00e9gorisation n'est recalcul\u00e9e : seules les valeurs saisies dans l'ERP sont expos\u00e9es.\n[GRAIN] 1 ligne par task_id (1 appel SAV). ~340 lignes, surtout depuis juillet 2026.\n[NOTES] appel_date = date(appel_start_at) sans conversion de fuseau : l'ERP stocke l'heure locale (Paris) \u00e9tiquet\u00e9e UTC ; l'heure affich\u00e9e correspond \u00e0 celle des exports ERP. resources_id est NULL quand aucune personne n'est affect\u00e9e \u00e0 l'appel (~25 %). L'\u00e9cran ERP affiche alors un libell\u00e9 d\u00e9riv\u00e9 de la cat\u00e9gorie (TECHNIQUE, MAGASIN, COMMERCE\u2026) : ce n'est pas une donn\u00e9e en base, il n'est pas reproduit ici \u2014 la cat\u00e9gorie porte l'information. device_id est NULL pour les appels sans machine (commande, RH, commerce\u2026). Un appel peut porter plusieurs sous-cat\u00e9gories : concat\u00e9n\u00e9es avec ' / ' (cf. intermediate). Les colonnes de l'Excel de suivi calcul\u00e9es par mots-cl\u00e9s sur le commentaire (cat\u00e9gorie appro, type technique) ne sont volontairement pas reproduites. appel_end_at est aujourd'hui toujours \u00e9gale \u00e0 appel_start_at : l'ERP n'enregistre pas de dur\u00e9e d'appel. Colonne conserv\u00e9e pour le jour o\u00f9 la fin sera saisie. Les 5 appels ant\u00e9rieurs \u00e0 juillet 2026 (ASAV2 \u00e0 ASAV6, sans cat\u00e9gorie) sont gard\u00e9s : ce sont des saisies ERP r\u00e9elles ; leur suppression \u00e9ventuelle se fait dans l'ERP.\n"""
    )
    as (
      
-- Test CI/CD live 2026-09-25 : commentaire sans effet sur les données, à retirer.

with appels as (
    select * from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_lcdp__appel_sav_tasks`
),

ressources as (
    select
        resources_id,
        resources_code,
        resources_name
    from `evs-datastack-prod`.`prod_marts`.`dim_lcdp__resource`
)

select
    -- Grain
    date(a.task_start_date) as appel_date,
    a.task_id,
    a.company_id,
    a.device_id,
    a.resources_id,

    -- Attributs d'affichage
    a.document_number as appel_numero,
    a.task_start_date as appel_start_at,
    a.task_end_date as appel_end_at,
    a.company_code,
    a.company_name,
    a.device_code,
    a.device_name,
    r.resources_code,
    r.resources_name,

    -- Catégorisation saisie dans l'ERP
    a.appel_categorie_code,
    a.appel_categorie_label,
    a.appel_sous_categorie_code,
    a.appel_sous_categorie_label,
    a.appel_detail_label,
    a.task_status_code,

    -- Commentaires
    a.comments_self,
    a.comments_peer,

    -- Métadonnées
    a.created_at,
    a.updated_at,
    a.extracted_at

from appels as a
left join ressources as r
    on a.resources_id = r.resources_id
    );
  