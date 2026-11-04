-- Règle : un fait ne lit pas un autre fait au même grain fin (docs/conventions/marts.md § 3).
-- Admis : agréger le fait lu, l'étendre en 1 pour 1, ou le croiser après agrégation au même
-- grain. La règle ne sait pas distinguer ces cas : chaque cas admis est listé ici avec sa
-- raison. Un nouveau cas arrête le build tant qu'il n'est ni corrigé ni ajouté à la liste.
with admis (fait, fait_lu) as (
    values
        -- agrégation du stock du jour par entité → société × article
        ('fct_supply_chain__couverture_stock_neshu', 'fct_supply_chain__stock_neshu'),
        -- agrégation du stock au mois
        ('fct_supply_chain__disponibilite_article_lcdp_depot_mensuel', 'fct_supply_chain__stock_lcdp'),
        ('fct_supply_chain__disponibilite_article_neshu_depot_mensuel', 'fct_supply_chain__stock_neshu'),
        ('fct_supply_chain__disponibilite_article_neshu_vehicule_mensuel', 'fct_supply_chain__stock_neshu'),
        -- classe de l'article lue sur sa clé unique (société × article) : pas de démultiplication
        ('fct_supply_chain__erreur_prevision_neshu', 'fct_supply_chain__classification_article_neshu'),
        -- même grain société × article (1 pour 1)
        ('fct_supply_chain__prevision_demande_neshu', 'fct_supply_chain__classification_article_neshu'),
        ('fct_supply_chain__point_commande_neshu', 'fct_supply_chain__prevision_demande_neshu'),
        -- ramené au grain société × article avant le croisement
        ('fct_supply_chain__point_commande_neshu', 'fct_supply_chain__erreur_prevision_neshu'),
        ('fct_supply_chain__point_commande_neshu', 'fct_supply_chain__stock_neshu'),
        -- consommation agrégée au jour × dépôt × référence
        ('fct_supply_chain__rupture_depot_yuman', 'fct_technique__consommation_article_nespresso'),
        ('fct_supply_chain__rupture_depot_yuman', 'fct_technique__consommation_article_yuman'),
        -- extension 1 pour 1 (même clé `key_inter`)
        ('fct_technique__intervention_retraitee', 'fct_technique__intervention')
)

select c.name as fait, p.name as fait_lu
from {{ info_schema('edges') }} as e
inner join {{ info_schema('models') }} as c on c.unique_id = e.child_unique_id
inner join {{ info_schema('models') }} as p on p.unique_id = e.parent_unique_id
where c.original_file_path like 'models/marts/%'
  and c.name like 'fct\_%' escape '\'
  and p.original_file_path like 'models/marts/%'
  and p.name like 'fct\_%' escape '\'
  and not exists (
      select 1 from admis as a
      where a.fait = c.name and a.fait_lu = p.name
  )
