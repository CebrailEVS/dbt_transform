

-- Prix unitaire des pièces Nespresso (système Nomad), un par référence, pour les pièces posées lors des
-- interventions terminées des agences EVS. Un seul prix par référence dans le référentiel de prix.
select distinct
    p.article_ref_nomad as piece_ref_nomad,
    p.article_prix_unitaire as piece_prix_unitaire
from `evs-datastack-prod`.`prod_intermediate`.`int_nesp_tech__articles_dedup` as a
inner join `evs-datastack-prod`.`prod_intermediate`.`int_nesp_tech__interventions_dedup` as i
    on a.n_planning = i.n_planning
inner join `evs-datastack-prod`.`prod_reference`.`ref_nesp_tech__articles_prix` as p
    on a.code_article = lower(p.article_ref_nomad)
where
    i.etat_intervention in ('terminée signée', 'signature différée')
    and i.agency in ('evs', 'evs paris', 'evs idf', 'evs paris 2')
    and p.article_prix_unitaire is not null