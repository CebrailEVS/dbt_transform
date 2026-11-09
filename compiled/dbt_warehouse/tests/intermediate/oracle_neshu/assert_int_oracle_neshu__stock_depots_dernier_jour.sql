-- Dépôt de distribution sans aucune ligne de stock au dernier jour d'extraction. Le point de commande
-- lit ce jour-là et compte 0 un article absent : un dépôt manquant passerait tout entier en rupture.
-- Rattaché au point de commande : en échec, app_cockpit__neshu_point_commande n'est pas reconstruit et
-- l'app garde la veille (dbt ne bloque que l'aval de tous les parents du test).
-- depends_on: `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__point_commande`
with stock_depot as (
    select
        id_entity,
        snapshot_date
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__stock_theorique`
    where
        entity_type = 'company'
        and id_entity in (114, 116, 118, 364)
),

dernier_jour as (
    select max(snapshot_date) as snapshot_date
    from stock_depot
),

depot_present as (
    select distinct s.id_entity
    from stock_depot as s
    inner join dernier_jour as j on s.snapshot_date = j.snapshot_date
),

depot_attendu as (
    select id_entity
    from unnest([114, 116, 118, 364]) as id_entity
)

select
    d.id_entity,
    j.snapshot_date as dernier_jour
from depot_attendu as d
cross join dernier_jour as j
left join depot_present as p on d.id_entity = p.id_entity
where p.id_entity is null