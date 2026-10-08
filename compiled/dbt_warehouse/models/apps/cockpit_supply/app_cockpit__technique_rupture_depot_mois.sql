

-- Historique des ruptures TechCare, partie comptée : nombre de jours suivis par mois,
-- dépôt et référence (dénominateur du taux de disponibilité) et dernier jour suivi.
-- Même fenêtre et mêmes règles fournisseur / code article que
-- app_cockpit__technique_rupture_depot_jour_vide.
with normalisees as (
    select
        stock_date,
        depot,
        trim(reference) as reference,
        case
    when starts_with(trim(reference), 'EVS_NESPRESSO_') or starts_with(trim(reference), 'EVS_NESP_') then 'Nespresso'
    when starts_with(trim(reference), 'EVS_NESTLE_') then 'Nestlé'
    when starts_with(trim(reference), 'ANIM_') then 'Animo'
    when starts_with(trim(reference), 'EVS_BRITA_') then 'Brita'
    when starts_with(trim(reference), 'BRIT') then 'Brita_GF'
    when starts_with(trim(reference), 'TWYD') or starts_with(trim(reference), 'TYWD') then 'TYWD'
    when starts_with(trim(reference), 'AUUM_') then 'AUUM'
end as fournisseur
    from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__rupture_depot_yuman`
    where stock_date >= date_trunc(date_sub(current_date('Europe/Paris'), interval 1 year), year)
)
select
    format_date('%Y-%m', stock_date) as mois,
    depot,
    fournisseur,
    upper(case
    when fournisseur = 'Nespresso' then replace(replace(reference, 'EVS_NESPRESSO_', ''), 'EVS_NESP_', '')
    when fournisseur = 'Nestlé' then replace(reference, 'EVS_NESTLE_', '')
    else reference
end) as code_article,
    reference,
    count(*) as nb_jours,
    max(stock_date) as derniere_date
from normalisees
where fournisseur is not null
group by mois, depot, fournisseur, code_article, reference