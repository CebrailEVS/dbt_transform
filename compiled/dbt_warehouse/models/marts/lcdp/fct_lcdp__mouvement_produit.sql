

-- Mouvements de stock produit en machine (LCDP), au grain produit × machine × jour × roadman.
-- Trois mesures de volume exposées séparément : une entrée (qty_chargee) et DEUX
-- canaux de sortie distincts, sans double comptage — qty_retiree (produit ressorti
-- pendant le passage de chargement, saisi en négatif : le sens vient du SIGNE, cf.
-- movement_type en amont) et qty_invendus (constat dédié, task_type 11).
-- TOUT LE PARC : aucun filtre de type de machine ici, le périmètre se choisit en BI
-- via la relation vers dim_lcdp__device (device_category, audit_type, currency_mode).
-- Détail du périmètre, de la valorisation et des pièges de lecture : voir la
-- description YAML du modèle, qui fait foi.

with chargement_daily as (
    select
        date(c.task_start_date) as mouvement_date,
        c.device_id,
        c.product_id,
        c.roadman_id,
        max(c.company_id) as company_id,
        sum(case when c.movement_type = 'LOADING' then c.load_quantity else 0 end)
            as qty_chargee,
        sum(case when c.movement_type = 'REMOVING' then -c.load_quantity else 0 end)
            as qty_retiree,
        sum(case when c.movement_type = 'LOADING' then c.load_valuation else 0 end)
            as montant_charge_eur,
        sum(case when c.movement_type = 'REMOVING' then -c.load_valuation else 0 end)
            as montant_retire_eur,
        max(c.updated_at) as updated_at
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_lcdp__chargement_tasks` as c
    where
        -- Même définition de « réalisé » que les invendus en amont, qui ne retiennent
        -- que FAIT / VALIDE : l'intermediate chargement remonte aussi ANNULE et
        -- ANOMALIE, le filtre est donc posé ici.
        c.task_status_code in ('FAIT', 'VALIDE')
        -- Retraits saisis au niveau du client sans machine renseignée (erreur de
        -- saisie ERP, ~50 lignes) : non rattachables au grain machine.
        and c.device_id is not null
    group by 1, 2, 3, 4
),

invendus_daily as (
    select
        date(i.task_start_date) as mouvement_date,
        i.device_id,
        i.product_id,
        i.roadman_id,
        max(i.company_id) as company_id,
        sum(i.quantity) as qty_invendus,
        sum(i.valuation) as montant_invendus_eur,
        max(i.updated_at) as updated_at
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_lcdp__invendus_tasks` as i
    where i.device_id is not null
    group by 1, 2, 3, 4
),

-- FULL OUTER JOIN obligatoire : plus d'un tiers des constats d'invendus n'ont pas
-- de chargement sur le même produit × machine × jour. Un LEFT JOIN sur le
-- chargement les perdrait.
-- Le roadman fait partie de la clé : chargement et invendus d'un même passage
-- tombent sur la même ligne (même roadman dans 99,4 % des cas) ; quand deux
-- personnes différentes interviennent, chacune a sa ligne. NULL-safe sur le
-- roadman (tâche sans ressource PERSON) pour ne pas dédoubler ces lignes.
mouvements as (
    select
        coalesce(c.mouvement_date, i.mouvement_date) as mouvement_date,
        coalesce(c.device_id, i.device_id) as device_id,
        coalesce(c.product_id, i.product_id) as product_id,
        coalesce(c.roadman_id, i.roadman_id) as roadman_id,
        coalesce(c.company_id, i.company_id) as company_id,

        coalesce(c.qty_chargee, 0) as qty_chargee,
        coalesce(c.qty_retiree, 0) as qty_retiree,
        coalesce(i.qty_invendus, 0) as qty_invendus,

        coalesce(c.montant_charge_eur, 0) as montant_charge_eur,
        coalesce(c.montant_retire_eur, 0) as montant_retire_eur,
        coalesce(i.montant_invendus_eur, 0) as montant_invendus_eur,

        greatest(
            coalesce(c.updated_at, timestamp('1970-01-01')),
            coalesce(i.updated_at, timestamp('1970-01-01'))
        ) as updated_at

    from chargement_daily as c
    full outer join invendus_daily as i
        on
            c.mouvement_date = i.mouvement_date
            and c.device_id = i.device_id
            and c.product_id = i.product_id
            and coalesce(c.roadman_id, -1) = coalesce(i.roadman_id, -1)
)

select
    m.mouvement_date,
    m.device_id,
    m.product_id,
    m.roadman_id as resources_roadman_id,
    m.company_id,

    -- Attributs d'affichage aplatis depuis les dims parentes (code + libellé
    -- seulement) : évitent une jointure côté BI sans dupliquer les dimensions.
    -- company_code / company_name viennent de dim_lcdp__company via le client
    -- porté par la TÂCHE, pas du client courant de la machine aplati sur
    -- dim_lcdp__device — une machine peut changer de client dans le temps.
    d.device_code,
    d.device_name,
    p.product_code,
    p.product_name,
    r.resources_code as roadman_code,
    r.resources_name as roadman_name,
    co.company_code,
    co.company_name,

    m.qty_chargee,
    m.qty_retiree,
    m.qty_invendus,

    m.montant_charge_eur,
    m.montant_retire_eur,
    m.montant_invendus_eur,

    m.updated_at

from mouvements as m
left join `evs-datastack-prod`.`prod_marts`.`dim_lcdp__device` as d on m.device_id = d.device_id
left join `evs-datastack-prod`.`prod_marts`.`dim_lcdp__product` as p on m.product_id = p.product_id
left join `evs-datastack-prod`.`prod_marts`.`dim_lcdp__resource` as r on m.roadman_id = r.resources_id
left join `evs-datastack-prod`.`prod_marts`.`dim_lcdp__company` as co on m.company_id = co.company_id