
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__lcdp_mouvement_machine`
      
    
    cluster by device_id

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Mouvements d'\u00e9quipements Caf\u00e9s du Phare (machines et sous-\u00e9quipements), tels que lus par l'application Cockpit Supply : installations, retours, ventes, transferts et r\u00e9ceptions, pour la cl\u00f4ture du parc neuf et l'\u00e9tat des \u00e9quipements \u00e0 une date.\n[COMMENT CONSTRUITE] stg_oracle_lcdp__task (type 134 \u00ab Mouvement mat\u00e9riel \u00bb, DEVICEMOV ; enregistrement actif, date renseign\u00e9e), \u00e9quipement de stg_oracle_lcdp__device avec son type et son article mod\u00e8le (comme app_cockpit__lcdp_machine), source et destination r\u00e9solues selon leur nature (soci\u00e9t\u00e9 typ\u00e9e par stg_oracle_lcdp__company_type, ou \u00e9quipement porteur), statut mat\u00e9riel de la t\u00e2che (famille STATUT_MATERIEL, libell\u00e9 fran\u00e7ais). movement_type classe le mouvement d'apr\u00e8s la nature de la source, celle de la destination et le statut mat\u00e9riel.\n[GRAIN] 1 ligne par task_id (t\u00e2che de mouvement), tous statuts.\n[NOTES] L'emplacement d'un \u00e9quipement \u00e0 une date est la destination de son dernier mouvement valid\u00e9 \u00e0 cette date ; un test le contr\u00f4le contre l'emplacement actuel de app_cockpit__lcdp_machine. created_at rep\u00e8re les mouvements saisis apr\u00e8s coup, qui modifient un mois pass\u00e9. Heures en heure de Paris. Noms de soci\u00e9t\u00e9s clientes seulement, aucun nom de personne. R\u00e9serv\u00e9 \u00e0 l'application, pas de rapport Power BI dessus.\n"""
    )
    as (
      

-- Mouvements d'équipements Cafés du Phare (tâches « Mouvement matériel », type 134 DEVICEMOV), pour Cockpit
-- Supply : installation, retour, vente, transfert, réception. Une ligne par tâche, tous statuts (l'app retient
-- VALIDE) ; l'emplacement d'un équipement à une date est la destination de son dernier mouvement validé.

with labels_fr as (
    select
        idstring,
        trim(text) as text
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__string`
    where langage_code = 'fr_FR'
),

-- Statut matériel après le mouvement (famille STATUT_MATERIEL) : au plus un par tâche.
statut_materiel as (
    select
        lht.idtask,
        la.code as material_status_code,
        la_fr.text as material_status
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__label_has_task` as lht
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__label` as la
        on lht.idlabel = la.idlabel
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__label_family` as lf
        on la.idlabel_family = lf.idlabel_family
    left join labels_fr as la_fr
        on la.idstring = la_fr.idstring
    where lf.code = 'STATUT_MATERIEL'
    qualify row_number() over (partition by lht.idtask order by la.code) = 1
),

-- Une société est un dépôt, un client, un fournisseur ou la société elle-même, selon son type Distrilog.
societes as (
    select
        c.idcompany,
        c.code,
        c.name,
        case ct.code
            when 'STOREHOUSE' then 'DEPOT'
            when 'CUSTOMER' then 'CLIENT'
            when 'SUPPLIER' then 'FOURNISSEUR'
            when 'SOCIETY' then 'SOCIETE'
            else 'AUTRE'
        end as company_kind
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__company` as c
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__company_type` as ct
        on c.idcompany_type = ct.idcompany_type
),

mouvements as (
    select
        t.idtask as task_id,
        t.document_number,
        t.real_start_date,
        t.created_at,
        ts.code as task_status_code,

        d.iddevice as device_id,
        d.code as device_code,
        d.serial as device_serial_number,
        d.name as device_name,
        dt.code as device_type,
        m.code as product_code,
        m.name as product_name,

        -- Source et destination : une société (dépôt, client…), un équipement porteur (sous-équipement) ou rien.
        case t.type_product_source
            when 'COMPANY' then coalesce(cs.company_kind, 'AUTRE')
            when 'DEVICE' then 'EQUIPEMENT'
            else 'AUCUNE'
        end as source_kind,
        case t.type_product_source
            when 'COMPANY' then cs.code
            when 'DEVICE' then ds.code
        end as source_code,
        case t.type_product_source
            when 'COMPANY' then cs.name
            when 'DEVICE' then ds.name
        end as source_name,
        case t.type_product_destination
            when 'COMPANY' then coalesce(cd.company_kind, 'AUTRE')
            when 'DEVICE' then 'EQUIPEMENT'
            else 'AUCUNE'
        end as destination_kind,
        case t.type_product_destination
            when 'COMPANY' then cd.code
            when 'DEVICE' then dd.code
        end as destination_code,
        case t.type_product_destination
            when 'COMPANY' then cd.name
            when 'DEVICE' then dd.name
        end as destination_name,

        sm.material_status_code,
        sm.material_status

    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__task` as t
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__device` as d
        on t.iddevice = d.iddevice
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__device_type` as dt
        on d.iddevice_type = dt.iddevice_type
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__product` as m
        on d.idmodel = m.idproduct
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__task_status` as ts
        on t.idtask_status = ts.idtask_status
    left join societes as cs
        on t.type_product_source = 'COMPANY' and t.idproduct_source = cs.idcompany
    left join societes as cd
        on t.type_product_destination = 'COMPANY' and t.idproduct_destination = cd.idcompany
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__device` as ds
        on t.type_product_source = 'DEVICE' and t.idproduct_source = ds.iddevice
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__device` as dd
        on t.type_product_destination = 'DEVICE' and t.idproduct_destination = dd.iddevice
    left join statut_materiel as sm
        on t.idtask = sm.idtask
    where
        t.idtask_type = 134  -- INSTALL MACHINE (« Mouvement matériel », DEVICEMOV)
        and t.code_status_record = '1'
        and t.real_start_date is not null
)

select
    task_id,
    document_number,
    timestamp(datetime(real_start_date, 'Europe/Paris')) as movement_at,
    timestamp(datetime(created_at, 'Europe/Paris')) as created_at,
    task_status_code,
    device_id,
    device_code,
    device_serial_number,
    device_name,
    device_type,
    product_code,
    product_name,
    source_kind,
    source_code,
    source_name,
    destination_kind,
    destination_code,
    destination_name,
    material_status_code,
    material_status,
    -- Classement métier du mouvement : le premier cas qui s'applique l'emporte.
    case
        when destination_kind = 'EQUIPEMENT' then 'Sous-équipement monté'
        when source_kind = 'EQUIPEMENT' then 'Sous-équipement démonté'
        when source_kind = 'AUCUNE' then 'Création de la fiche'
        when material_status_code = 'EV' and destination_kind in ('CLIENT', 'SOCIETE') then 'Vente'
        when source_kind = 'FOURNISSEUR' and destination_kind = 'DEPOT' then 'Réception'
        when source_kind = 'DEPOT' and destination_kind in ('CLIENT', 'SOCIETE') then 'Installation'
        when source_kind in ('CLIENT', 'SOCIETE') and destination_kind = 'DEPOT' and material_status_code = 'CA'
            then 'Retour en casse'
        when source_kind in ('CLIENT', 'SOCIETE') and destination_kind = 'DEPOT' then 'Retour'
        when source_kind = 'DEPOT' and destination_kind = 'DEPOT' and material_status_code = 'CA' then 'Mise en casse'
        when source_kind = 'DEPOT' and destination_kind = 'DEPOT' then 'Transfert entre dépôts'
        when source_kind in ('CLIENT', 'SOCIETE') and destination_kind in ('CLIENT', 'SOCIETE')
            then 'Transfert entre clients'
        else 'Autre'
    end as movement_type
from mouvements
    );
  