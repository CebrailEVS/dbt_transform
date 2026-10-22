
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__neshu_equipement_stock`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] \u00c9quipements Neshu en stock pour l'\u00e9cran Supply \u00ab \u00c9quipements en stock \u00bb de Cockpit Supply : machines et terminaux de paiement pr\u00e9sents dans les d\u00e9p\u00f4ts et ateliers, avec leur statut (disponible, r\u00e9serv\u00e9, \u00e0 remettre en \u00e9tat, inactif \u00e0 investiguer, rebut), leur valeur, leur anciennet\u00e9 en stock et les contr\u00f4les qualit\u00e9 de la donn\u00e9e ; plus les machines command\u00e9es et pas encore re\u00e7ues.\n[COMMENT CONSTRUITE] Depuis le staging, sans les marts. \u00c9quipements : stg_oracle_neshu__device de type Machine (1) ou paiement (4), rattach\u00e9s aux d\u00e9p\u00f4ts 01, 06, 07, 08, 09, 10 (rebut), 11 (remise en \u00e9tat), 13 et 14 ; labels ISACTIVE, EQUIPEMENT_RESERVE, MARQUE et CATEGORIE ; mod\u00e8le et prix de fiche article par stg_oracle_neshu__product ; classe du mod\u00e8le par le seed ref_oracle_neshu__classe_modele_machine (classification du Supply). Commandes : int_oracle_neshu__commande_fournisseur_tasks sur les produits d'\u00e9quipement (type 4), faites ou valid\u00e9es, livraison EN_ATTENTE ou LIVRE_PARTIEL ; re\u00e7u = lignes de BL fournisseur (stg_oracle_neshu__task_has_product, type 121) qui pointent vers la ligne de commande (idtask_has_product_associed).\n[GRAIN] 1 ligne par ligne_id : un \u00e9quipement (device_id) ou une ligne de commande non sold\u00e9e.\n[NOTES] R\u00e8gles valid\u00e9es avec le Supply : statut = Rebut au d\u00e9p\u00f4t 10, Inactif hors rebut (\u00e0 investiguer) si inactif ailleurs, \u00c0 remettre en \u00e9tat si actif au 11, R\u00e9serv\u00e9 si label EQUIPEMENT_RESERVE = OUI, Disponible sinon ; une commande de plus de 12 mois est \u00ab \u00c0 solder \u00bb. Un terminal de paiement inactif est \u00ab Inactif \u00bb, sans anomalie. Cat\u00e9gorie : la classe du mod\u00e8le (seed) prime sur le type Distrilog, d'o\u00f9 RHEA IRDA (module de relev\u00e9 infrarouge typ\u00e9 paiement) en composant ; un module ou composant sans parent est \u00ab Composant en stock \u00bb, rattach\u00e9 \u00e0 un parent \u00ab Composant rattach\u00e9 \u00bb ; un mod\u00e8le absent de la classification est \u00ab \u00c0 classer \u00bb. Valeur : valeur_comptee ne retient que le mat\u00e9riel propri\u00e9t\u00e9 d'EVS (et les commandes) ; valeur reste affich\u00e9e pour information. Doublons de code contr\u00f4l\u00e9s sur tout le parc, doublons de s\u00e9rie entre \u00e9quipements actifs du p\u00e9rim\u00e8tre. R\u00e9serv\u00e9 \u00e0 l'application.\n"""
    )
    as (
      

-- Équipements Neshu en stock (écran Supply « Équipements en stock ») : une ligne par machine ou terminal de
-- paiement présent dans un dépôt, plus une ligne par ligne de commande fournisseur d'équipement non soldée.
-- Règles validées avec le Supply : base = type Machine de Distrilog (plus les terminaux de paiement à part),
-- dépôts 01, 06, 07, 08, 09, 11, 13, 14, le 10 (rebut) servant aux contrôles.

with depot as (
    -- Rôle du dépôt dans l'écran : le 11 est la remise en état, le 10 le rebut, les 01 et 13 des dépôts produits.
    select
        idcompany,
        code as depot_code,
        name as depot_name,
        left(name, 2) as depot_numero,
        case
            when code = 'DEPOTREBUS' then 'rebut'
            when code = 'RERUNGIS' then 'remise_en_etat'
            when code in ('DEPOTRUNGIS', 'DEPOTMARSEILLE') then 'produits'
            else 'atelier'
        end as depot_role
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__company`
    where
        code in (
            'DEPOTRUNGIS', 'DEPOTATELIERRUNGIS', 'DEPOTATELIERLYON', 'DEPOTATELIERBORDEAUX',
            'DEPOTATELIERSTRASBOURG', 'DEPOTREBUS', 'RERUNGIS', 'DEPOTMARSEILLE', 'DEPOTATELIERMARSEILLE'
        )
        and code_status_record != -1
),

parc as (
    -- Tout le parc hors ghost-deletes : sert aux contrôles de doublons et de cohérence parent / enfant.
    select
        d.iddevice,
        d.device_iddevice,
        d.iddevice_type,
        d.code,
        nullif(trim(d.serial), '') as serial,
        coalesce(lower(max(case when lf.code = 'ISACTIVE' then l.code end)) = 'yes', false) as is_active,
        coalesce(upper(max(case when lf.code = 'EQUIPEMENT_RESERVE' then l.code end)) = 'OUI', false) as is_reserved,
        max(case when lf.code = 'MARQUE' then l.code end) as device_brand,
        max(case when lf.code = 'CATEGORIE' then l.code end) as categorie_distrilog
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__device` as d
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label_has_device` as lhd
        on d.iddevice = lhd.iddevice
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label` as l
        on lhd.idlabel = l.idlabel
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label_family` as lf
        on l.idlabel_family = lf.idlabel_family
    where d.code_status_record != -1
    group by d.iddevice, d.device_iddevice, d.iddevice_type, d.code, d.serial
),

-- Catégorie d'un modèle hors classification : déduite du type des équipements qui portent ce modèle.
type_par_modele as (
    select
        d.idmodel,
        max(d.iddevice_type) as iddevice_type
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__device` as d
    where d.code_status_record != -1 and d.idmodel is not null
    group by d.idmodel
    having count(distinct d.iddevice_type) = 1
),

classe as (
    select
        model_code,
        classe_machine
    from `evs-datastack-prod`.`prod_reference`.`ref_oracle_neshu__classe_modele_machine`
),

equipement as (
    select
        d.iddevice as device_id,
        d.code as device_code,
        d.name as device_name,
        p.code as model_code,
        p.name as model_name,
        d.iddevice_type as device_type_id,
        cl.classe_machine as classe_modele,
        pc.serial as serial_number,
        pc.device_brand,
        pc.categorie_distrilog,
        par.code as parent_device_code,
        dp.depot_code,
        dp.depot_name,
        dp.depot_numero,
        dp.depot_role,
        pc.is_active,
        pc.is_reserved,
        o.name as owner_name,
        case
            when o.idcompany is null then 'Non renseigné'
            when o.code = 'NESTLE' then 'Nestlé'
            when o.code = 'NESPRESSO' then 'Nespresso'
            when o.idcompany_type = 4 then 'EVS'
            when o.idcompany_type = 1 then 'Fournisseur'
            when o.idcompany_type = 2 then 'Client'
            else 'Autre'
        end as owner_categorie,
        d.comments as device_comment,
        d.purchase_date,
        -- Un prix à 0 n'est pas distingué d'un prix non saisi à la source : il est traité comme absent.
        nullif(d.purchase_cost, 0) as prix_achat_machine,
        nullif(p.purchase_unit_price, 0) as prix_achat_modele,
        -- Entrée en stock = dernière installation (Distrilog l'« installe » au dépôt), sinon date d'achat.
        coalesce(date(d.last_installation_date, 'Europe/Paris'), d.purchase_date) as date_entree_stock,
        d.device_iddevice is not null as is_rattache_parent,
        -- Terminal de paiement = type paiement de Distrilog, sauf modèle classé composant par le Supply
        -- (RHEA IRDA : module de relevé infrarouge, typé paiement dans l'ERP).
        d.iddevice_type = 4 and coalesce(cl.classe_machine, '') != 'Module / composant' as is_terminal_paiement,
        countif(pc.is_active) over (partition by d.iddevice_type, pc.serial) as nb_actifs_meme_serie,
        coalesce(par.is_active = false and pc.is_active, false) as is_actif_sous_parent_inactif
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__device` as d
    inner join depot as dp
        on d.idcompany_customer = dp.idcompany
    inner join parc as pc
        on d.iddevice = pc.iddevice
    left join parc as par
        on d.device_iddevice = par.iddevice
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__product` as p
        on d.idmodel = p.idproduct
    left join classe as cl
        on p.code = cl.model_code
    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__company` as o
        on d.idcompany_owner = o.idcompany
    where
        d.code_status_record != -1
        and d.iddevice_type in (1, 4)  -- machines et terminaux de paiement
),

code_global as (
    -- Un code équipement doit être unique dans tout le parc, pas seulement dans les dépôts.
    select code
    from parc
    group by code
    having count(*) > 1
),

parent_avec_enfant_actif as (
    select distinct device_iddevice as iddevice
    from parc
    where device_iddevice is not null and is_active
),

equipement_qualifie as (
    select
        e.*,
        case
            when e.classe_modele = 'Module / composant' and e.is_rattache_parent then 'Composant rattaché'
            when e.classe_modele = 'Module / composant' then 'Composant en stock'
            when e.is_terminal_paiement then 'Terminal de paiement'
            when e.classe_modele is not null then e.classe_modele
            else 'À classer'
        end as categorie,
        -- Un terminal inactif n'est pas une anomalie (règle du Supply) : statut « Inactif », à part.
        case
            when e.depot_role = 'rebut' then 'Rebut'
            when not e.is_active and e.is_terminal_paiement then 'Inactif'
            when not e.is_active then 'Inactif hors rebut (à investiguer)'
            when e.depot_role = 'remise_en_etat' then 'À remettre en état'
            when e.is_reserved then 'Réservé'
            else 'Disponible'
        end as statut,
        -- Contrôles qualité de la donnée (règles validées avec le Supply).
        cg.code is not null as is_code_double,
        e.serial_number is not null and e.is_active and e.nb_actifs_meme_serie > 1 as is_serie_double,
        not e.is_active and e.depot_role != 'rebut' and not e.is_terminal_paiement as is_inactif_hors_rebut,
        e.is_active and e.depot_role = 'rebut' as is_actif_au_rebut,
        e.owner_name is null as is_sans_proprietaire,
        e.serial_number is null as is_sans_serie,
        e.depot_role = 'produits' as is_equipement_depot_produits,
        e.device_type_id = 1 and e.is_rattache_parent as is_machine_rattachee_parent,
        (not e.is_active and pa.iddevice is not null) or e.is_actif_sous_parent_inactif
            as is_incoherence_parent_enfant
    from equipement as e
    left join code_global as cg
        on e.device_code = cg.code
    left join parent_avec_enfant_actif as pa
        on e.device_id = pa.iddevice
),

-- Commandes fournisseurs d'équipements (produits de type 4) faites ou validées, pas entièrement livrées.
commande as (
    select
        c.task_product_id,
        c.task_id,
        c.product_code,
        p.name as product_name,
        dp.depot_code,
        dp.depot_name,
        dp.depot_numero,
        dp.depot_role,
        date(c.task_start_date, 'Europe/Paris') as date_commande,
        c.quantity as quantite_commandee,
        coalesce(nullif(c.product_unit_price_task, 0), nullif(p.purchase_unit_price, 0)) as prix_unitaire,
        case
            when nullif(c.product_unit_price_task, 0) is not null then 'commande'
            when nullif(p.purchase_unit_price, 0) is not null then 'modèle'
            else 'aucun'
        end as source_prix,
        cl.classe_machine as classe_modele,
        tm.iddevice_type as type_modele
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__commande_fournisseur_tasks` as c
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__product` as p
        on c.product_id = p.idproduct
    inner join depot as dp
        on c.product_destination_id = dp.idcompany
    left join classe as cl
        on c.product_code = cl.model_code
    left join type_par_modele as tm
        on c.product_id = tm.idmodel
    where
        p.idproduct_type = 4
        and c.task_status_code in ('FAIT', 'VALIDE')
        and c.delivery_status_code in ('EN_ATTENTE', 'LIVRE_PARTIEL')
),

reception as (
    -- Une ligne de bon de réception pointe vers la ligne de commande qu'elle livre.
    select
        thp.idtask_has_product_associed as task_product_id,
        sum(thp.real_quantity * thp.unit_coeff_multi / thp.unit_coeff_div) as quantite_recue
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task_has_product` as thp
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task` as t
        on thp.idtask = t.idtask
    where
        t.idtask_type = 121  -- BL FOURNISSEUR
        and t.code_status_record = '1'
        and thp.idtask_has_product_associed in (select cmd.task_product_id from commande as cmd)
    group by thp.idtask_has_product_associed
),

commande_reste as (
    select
        c.*,
        coalesce(r.quantite_recue, 0) as quantite_recue,
        greatest(c.quantite_commandee - coalesce(r.quantite_recue, 0), 0) as quantite_restante
    from commande as c
    left join reception as r
        on c.task_product_id = r.task_product_id
)

select
    'equipement' as ligne_type,
    concat('equipement-', cast(device_id as string)) as ligne_id,
    device_id,
    device_code,
    device_name,
    model_code,
    model_name,
    device_type_id,
    classe_modele,
    categorie,
    categorie_distrilog,
    categorie in ('Machine principale', 'Machine (comptoir ou à monter)') as is_compte_machine,
    serial_number,
    device_brand,
    parent_device_code,
    depot_code,
    depot_name,
    depot_numero,
    statut,
    is_active,
    is_reserved,
    if(is_reserved, device_comment, null) as client_reservataire,
    owner_name,
    owner_categorie,
    device_comment,
    purchase_date,
    date_entree_stock,
    date_diff(current_date('Europe/Paris'), date_entree_stock, day) as anciennete_jours,
    cast(null as int64) as commande_task_id,
    cast(null as date) as date_commande,
    cast(null as float64) as quantite_commandee,
    cast(null as float64) as quantite_recue,
    1.0 as quantite,
    prix_achat_machine,
    prix_achat_modele,
    coalesce(prix_achat_machine, prix_achat_modele) as valeur,
    -- Seul le matériel propriété d'EVS entre dans les totaux de valeur ; les autres propriétaires sont
    -- affichés en quantité, leur valeur à titre d'information (règle du Supply).
    if(owner_categorie = 'EVS', coalesce(prix_achat_machine, prix_achat_modele), 0) as valeur_comptee,
    case
        when prix_achat_machine is not null then 'machine'
        when prix_achat_modele is not null then 'modèle'
        else 'aucun'
    end as source_prix,
    is_code_double,
    is_serie_double,
    is_inactif_hors_rebut,
    is_actif_au_rebut,
    is_sans_proprietaire,
    is_sans_serie,
    is_equipement_depot_produits,
    is_machine_rattachee_parent,
    is_incoherence_parent_enfant,
    is_code_double or is_serie_double or is_inactif_hors_rebut or is_actif_au_rebut or is_sans_proprietaire
    or is_sans_serie or is_equipement_depot_produits or is_machine_rattachee_parent
    or is_incoherence_parent_enfant as has_anomalie
from equipement_qualifie

union all

select
    'commande' as ligne_type,
    concat('commande-', cast(task_product_id as string)) as ligne_id,
    cast(null as int64) as device_id,
    cast(null as string) as device_code,
    product_name as device_name,
    product_code as model_code,
    product_name as model_name,
    cast(null as int64) as device_type_id,
    classe_modele,
    case
        when classe_modele = 'Module / composant' then 'Composant en stock'
        when type_modele = 4 then 'Terminal de paiement'
        when classe_modele is not null then classe_modele
        else 'À classer'
    end as categorie,
    cast(null as string) as categorie_distrilog,
    coalesce(classe_modele in ('Machine principale', 'Machine (comptoir ou à monter)'), false) as is_compte_machine,
    cast(null as string) as serial_number,
    cast(null as string) as device_brand,
    cast(null as string) as parent_device_code,
    depot_code,
    depot_name,
    depot_numero,
    -- Une commande de plus de 12 mois est à solder dans Distrilog, pas à attendre.
    if(date_commande < date_sub(current_date('Europe/Paris'), interval 12 month), 'À solder', 'Commandé') as statut,
    cast(null as bool) as is_active,
    false as is_reserved,
    cast(null as string) as client_reservataire,
    cast(null as string) as owner_name,
    cast(null as string) as owner_categorie,
    cast(null as string) as device_comment,
    cast(null as date) as purchase_date,
    cast(null as date) as date_entree_stock,
    cast(null as int64) as anciennete_jours,
    task_id as commande_task_id,
    date_commande,
    quantite_commandee,
    quantite_recue,
    quantite_restante as quantite,
    cast(null as float64) as prix_achat_machine,
    cast(null as float64) as prix_achat_modele,
    quantite_restante * prix_unitaire as valeur,
    quantite_restante * prix_unitaire as valeur_comptee,
    source_prix,
    false as is_code_double,
    false as is_serie_double,
    false as is_inactif_hors_rebut,
    false as is_actif_au_rebut,
    false as is_sans_proprietaire,
    false as is_sans_serie,
    false as is_equipement_depot_produits,
    false as is_machine_rattachee_parent,
    false as is_incoherence_parent_enfant,
    false as has_anomalie
from commande_reste
where quantite_restante > 0
    );
  