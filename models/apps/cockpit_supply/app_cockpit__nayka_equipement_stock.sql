{{ config(materialized='table') }}

-- Équipements Nayka en stock (écran Supply « Équipements en stock ») : une ligne par machine ou terminal de
-- paiement présent dans un dépôt, plus une ligne par ligne de commande fournisseur d'équipement non soldée.
-- Mêmes colonnes et mêmes règles que app_cockpit__neshu_equipement_stock. Dépôts validés avec le Supply :
-- Rungis et Dardilly = stock, MARKETING = hors stock (cadeaux clients, showroom), DEPOTREBUT = rebut.

with depot as (
    select
        idcompany,
        code as depot_code,
        name as depot_name,
        case
            when code = 'DEPOTREBUT' then 'rebut'
            when code = 'MARKETING' then 'hors_stock'
            else 'stock'
        end as depot_role
    from {{ ref('stg_oracle_nayka__company') }}
    where
        code in ('DEPOTLOGISTIQUERUNGIS', 'DEPOTLOGISTIQUEDARDILLY', 'MARKETING', 'DEPOTREBUT')
        and coalesce(code_status_record, 0) != -1
),

parc as (
    -- Tout le parc hors ghost-deletes : sert aux contrôles de doublons et de cohérence parent / enfant.
    select
        d.iddevice,
        d.device_iddevice,
        d.code,
        nullif(trim(d.serial), '') as serial,
        coalesce(lower(max(case when lb.lf_code = 'ISACTIVE' then lb.l_code end)) = 'yes', false) as is_active,
        coalesce(upper(max(case when lb.lf_code = 'EQUIPEMENT_RESERVE' then lb.l_code end)) = 'OUI', false)
            as is_reserved,
        max(case when lb.lf_code = 'MARQUE_FOURNISSEUR' then lb.l_code end) as device_brand,
        max(case when lb.lf_code = 'CATEGORIE' then lb.l_code end) as categorie_distrilog
    from {{ ref('stg_oracle_nayka__device') }} as d
    left join {{ ref('stg_oracle_nayka__v_label_device') }} as lb
        on d.iddevice = lb.iddevice
    where coalesce(d.code_status_record, 0) != -1
    group by d.iddevice, d.device_iddevice, d.code, d.serial
),

-- Classification validée par le Supply : le type Distrilog suffit chez Nayka (Machine = fontaine = machine
-- principale, Accessoire = Base Cabinet = composant, paiement = terminal).
type_par_modele as (
    select
        d.idmodel,
        max(d.iddevice_type) as iddevice_type
    from {{ ref('stg_oracle_nayka__device') }} as d
    where coalesce(d.code_status_record, 0) != -1 and d.idmodel is not null
    group by d.idmodel
    having count(distinct d.iddevice_type) = 1
),

equipement as (
    select
        d.iddevice as device_id,
        d.code as device_code,
        d.name as device_name,
        p.code as model_code,
        p.name as model_name,
        d.iddevice_type as device_type_id,
        pc.serial as serial_number,
        pc.device_brand,
        pc.categorie_distrilog,
        par.code as parent_device_code,
        dp.depot_code,
        dp.depot_name,
        dp.depot_role,
        pc.is_active,
        pc.is_reserved,
        o.name as owner_name,
        case
            when o.idcompany is null then 'Non renseigné'
            when o.idcompany_type = 4 then 'Nayka'
            when o.idcompany_type = 1 then 'Fournisseur'
            when o.idcompany_type = 2 then 'Client'
            else 'Autre'
        end as owner_categorie,
        d.comments as device_comment,
        d.purchase_date,
        -- Un prix à 0 n'est pas distingué d'un prix non saisi à la source : il est traité comme absent.
        cast(nullif(d.purchase_cost, 0) as float64) as prix_achat_machine,
        cast(nullif(p.purchase_unit_price, 0) as float64) as prix_achat_modele,
        -- Entrée en stock = dernière installation (Distrilog l'« installe » au dépôt), sinon date d'achat.
        coalesce(date(d.last_installation_date, 'Europe/Paris'), d.purchase_date) as date_entree_stock,
        d.device_iddevice is not null as is_rattache_parent,
        countif(pc.is_active) over (partition by d.iddevice_type, pc.serial) as nb_actifs_meme_serie,
        coalesce(par.is_active = false and pc.is_active, false) as is_actif_sous_parent_inactif
    from {{ ref('stg_oracle_nayka__device') }} as d
    inner join depot as dp
        on d.idcompany_customer = dp.idcompany
    inner join parc as pc
        on d.iddevice = pc.iddevice
    left join parc as par
        on d.device_iddevice = par.iddevice
    left join {{ ref('stg_oracle_nayka__product') }} as p
        on d.idmodel = p.idproduct
    left join {{ ref('stg_oracle_nayka__company') }} as o
        on d.idcompany_owner = o.idcompany
    where
        coalesce(d.code_status_record, 0) != -1
        and d.iddevice_type in (1, 4, 12)  -- fontaines, terminaux de paiement et Base Cabinet
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
            when e.device_type_id = 12 and e.is_rattache_parent then 'Composant rattaché'
            when e.device_type_id = 12 then 'Composant en stock'
            when e.device_type_id = 4 then 'Terminal de paiement'
            else 'Machine principale'
        end as categorie,
        case
            when e.depot_role = 'rebut' then 'Rebut'
            when e.depot_role = 'hors_stock' then 'Hors stock (marketing)'
            when not e.is_active and e.device_type_id = 4 then 'Inactif'
            when not e.is_active then 'Inactif hors rebut (à investiguer)'
            when e.is_reserved then 'Réservé'
            else 'Disponible'
        end as statut,
        -- Contrôles qualité de la donnée (mêmes règles que Neshu ; pas de dépôt produits chez Nayka).
        cg.code is not null as is_code_double,
        e.serial_number is not null and e.is_active and e.nb_actifs_meme_serie > 1 as is_serie_double,
        not e.is_active and e.depot_role != 'rebut' and e.device_type_id != 4 as is_inactif_hors_rebut,
        e.is_active and e.depot_role = 'rebut' as is_actif_au_rebut,
        e.owner_name is null as is_sans_proprietaire,
        e.serial_number is null as is_sans_serie,
        false as is_equipement_depot_produits,
        e.device_type_id = 1 and e.is_rattache_parent as is_machine_rattachee_parent,
        (not e.is_active and pa.iddevice is not null) or e.is_actif_sous_parent_inactif
            as is_incoherence_parent_enfant
    from equipement as e
    left join code_global as cg
        on e.device_code = cg.code
    left join parent_avec_enfant_actif as pa
        on e.device_id = pa.iddevice
),

statut_livraison as (
    select
        lht.idtask,
        max(case when lf.code = 'STATUT_LIVRAISON' then l.code end) as delivery_status_code
    from {{ ref('stg_oracle_nayka__label_has_task') }} as lht
    inner join {{ ref('stg_oracle_nayka__label') }} as l
        on lht.idlabel = l.idlabel
    inner join {{ ref('stg_oracle_nayka__label_family') }} as lf
        on l.idlabel_family = lf.idlabel_family
    group by lht.idtask
),

tache as (
    select
        t.idtask,
        t.real_start_date,
        t.idproduct_destination,
        t.type_product_destination,
        tt.code as task_type_code,
        ts.code as task_status_code
    from {{ ref('stg_oracle_nayka__task') }} as t
    inner join {{ ref('stg_oracle_nayka__task_type') }} as tt
        on t.idtask_type = tt.idtask_type
    left join {{ ref('stg_oracle_nayka__task_status') }} as ts
        on t.idtask_status = ts.idtask_status
    where
        tt.code in ('COMMANDE FOURNISSEUR', 'BL FOURNISSEUR')
        and t.code_status_record = 1
),

-- Commandes fournisseurs d'équipements (produits de type 4) faites ou validées, pas entièrement livrées.
commande as (
    select
        thp.idtask_has_product as task_product_id,
        t.idtask as task_id,
        p.code as product_code,
        p.name as product_name,
        dp.depot_code,
        dp.depot_name,
        date(t.real_start_date, 'Europe/Paris') as date_commande,
        cast(thp.real_quantity * thp.unit_coeff_multi / thp.unit_coeff_div as float64) as quantite_commandee,
        cast(coalesce(nullif(thp.net_price, 0), nullif(p.purchase_unit_price, 0)) as float64) as prix_unitaire,
        case
            when nullif(thp.net_price, 0) is not null then 'commande'
            when nullif(p.purchase_unit_price, 0) is not null then 'modèle'
            else 'aucun'
        end as source_prix,
        tm.iddevice_type as type_modele
    from tache as t
    inner join statut_livraison as sl
        on t.idtask = sl.idtask
    inner join depot as dp
        on t.type_product_destination = 'COMPANY' and t.idproduct_destination = dp.idcompany
    inner join {{ ref('stg_oracle_nayka__task_has_product') }} as thp
        on t.idtask = thp.idtask
    inner join {{ ref('stg_oracle_nayka__product') }} as p
        on thp.idproduct = p.idproduct
    left join type_par_modele as tm
        on thp.idproduct = tm.idmodel
    where
        t.task_type_code = 'COMMANDE FOURNISSEUR'
        and t.task_status_code in ('FAIT', 'VALIDE')
        and sl.delivery_status_code in ('EN_ATTENTE', 'LIVRE_PARTIEL')
        and p.idproduct_type = 4
),

reception as (
    -- Une ligne de bon de réception pointe vers la ligne de commande qu'elle livre.
    select
        thp.idtask_has_product_associed as task_product_id,
        cast(sum(thp.real_quantity * thp.unit_coeff_multi / thp.unit_coeff_div) as float64) as quantite_recue
    from {{ ref('stg_oracle_nayka__task_has_product') }} as thp
    inner join tache as t
        on thp.idtask = t.idtask
    where
        t.task_type_code = 'BL FOURNISSEUR'
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
    cast(null as string) as classe_modele,
    categorie,
    categorie_distrilog,
    categorie = 'Machine principale' as is_compte_machine,
    serial_number,
    device_brand,
    parent_device_code,
    depot_code,
    depot_name,
    cast(null as string) as depot_numero,
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
    -- Seul le matériel propriété de la société (Nayka) entre dans les totaux de valeur ; les autres
    -- propriétaires sont affichés en quantité, leur valeur à titre d'information (règle du Supply).
    if(owner_categorie = 'Nayka', coalesce(prix_achat_machine, prix_achat_modele), 0) as valeur_comptee,
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
    cast(null as string) as classe_modele,
    case
        when type_modele = 4 then 'Terminal de paiement'
        when type_modele = 1 then 'Machine principale'
        when type_modele = 12 then 'Composant en stock'
        else 'À classer'
    end as categorie,
    cast(null as string) as categorie_distrilog,
    coalesce(type_modele = 1, false) as is_compte_machine,
    cast(null as string) as serial_number,
    cast(null as string) as device_brand,
    cast(null as string) as parent_device_code,
    depot_code,
    depot_name,
    cast(null as string) as depot_numero,
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
