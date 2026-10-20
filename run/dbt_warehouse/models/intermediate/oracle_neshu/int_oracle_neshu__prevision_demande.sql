
  
    

    create or replace table `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__prevision_demande`
      
    
    cluster by company_id, product_id

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Pr\u00e9vision de demande mensuelle et journali\u00e8re par d\u00e9p\u00f4t et article Neshu, selon la m\u00e9thode rout\u00e9e par la classification.\n[COMMENT CONSTRUITE] int_oracle_neshu__classification_article \u00d7 int_oracle_neshu__demande_mensuelle : moyenne des N derniers mois complets (var prevision_nb_mois_moyenne), r\u00e9f\u00e9rence saisonni\u00e8re N-1, na\u00eff ou 0 si exclu.\n[GRAIN] 1 ligne par (company_id, product_id) \u2014 \u00e9tat courant recalcul\u00e9 \u00e0 chaque build.\n[NOTES] M\u00eame logique que fct_supply_chain__prevision_demande_neshu, dupliqu\u00e9e volontairement : l'application ne lit pas les marts (cf. docs/conventions/apps.md). Une \u00e9volution de r\u00e8gle se reporte dans les deux.\n"""
    )
    as (
      

-- Copie applicative de fct_supply_chain__prevision_demande_neshu : même logique, construite sur l'intermediate
-- et le staging pour que l'app Cockpit ne dépende pas des marts (cf. docs/conventions/apps.md).

-- Prévision de demande NESHU par dépôt et article (maillon 3 : le forecast lui-même).
-- Applique la méthode routée par la classification (② l'aiguilleur) :
--   moyenne_mobile / moyenne_mobile_surveille -> moyenne des N derniers mois COMPLETS ;
--   reference_saisonniere (saisonniers avec historique) -> demande du mois À VENIR l'an dernier
--     (fenêtre N-1 ± 1 mois) : anticipe le redémarrage de saison là où la MA récente sous-estime
--     (ex. barres d'hiver ≈ 0 en été → la MA prédit ~0 pour septembre alors que le N-1 donne le
--     vrai niveau de reprise) ;
--   naif (indetermine) -> moyenne des mois disponibles ; exclu (arrete/inactif) -> 0.
-- Une moyenne mobile = somme(fenêtre) / N : les mois sans demande comptent comme 0 (implicite),
-- donc pas de densification. Le mois courant, incomplet, est EXCLU (sinon sous-estimation).
-- Fenêtre paramétrable via var dbt prevision_nb_mois_moyenne (défaut 3).

with classification as (

    select
        company_id,
        product_id,
        company_code,
        company_name,
        product_code,
        product_name,
        statut_vie,
        classe_abc,
        methode_prevision
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__classification_article`

),

demande_fenetre as (

    -- Somme de la demande sur les N derniers mois COMPLETS (mois courant exclu),
    -- rattachée à chaque couple de la classification (0 si aucune demande sur la fenêtre).
    select
        c.company_id,
        c.product_id,
        coalesce(sum(s.quantite_demandee), 0) as demande_fenetre,
        count(s.demande_mois) as nb_mois_avec_demande
    from classification as c
    left join `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__demande_mensuelle` as s
        on
            c.company_id = s.company_id
            and c.product_id = s.product_id
            and s.demande_mois >= date_sub(
                date_trunc(current_date('Europe/Paris'), month), interval 3 month
            )
            and s.demande_mois < date_trunc(current_date('Europe/Paris'), month)
    group by c.company_id, c.product_id

),

demande_n1_saison as (

    -- Référence saisonnière : demande du mois À VENIR l'an dernier, fenêtre ± 1 mois (÷ 3 pour lisser).
    -- Cible = mois suivant ; centre N-1 = ce mois il y a 1 an. Pour juillet : fenêtre juil→sept N-1,
    -- centrée sur août N-1. Anticipe le redémarrage (la MA récente, hors-saison ≈ 0, ne le voit pas).
    select
        c.company_id,
        c.product_id,
        coalesce(sum(s.quantite_demandee), 0) / 3 as demande_saison_n1
    from classification as c
    left join `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__demande_mensuelle` as s
        on
            c.company_id = s.company_id
            and c.product_id = s.product_id
            and s.demande_mois between
            date_sub(date_trunc(current_date('Europe/Paris'), month), interval 12 month)
            and date_sub(date_trunc(current_date('Europe/Paris'), month), interval 10 month)
    group by c.company_id, c.product_id

),

prevision as (

    select
        c.company_id,
        c.product_id,
        c.company_code,
        c.company_name,
        c.product_code,
        c.product_name,
        c.statut_vie,
        c.classe_abc,
        c.methode_prevision,
        f.demande_fenetre,
        f.nb_mois_avec_demande,
        case
            when c.methode_prevision = 'exclu' then 0
            when c.methode_prevision = 'reference_saisonniere' then round(n1.demande_saison_n1, 2)
            when c.methode_prevision = 'naif'
                then round(safe_divide(f.demande_fenetre, greatest(f.nb_mois_avec_demande, 1)), 2)
            else round(f.demande_fenetre / 3, 2)
        end as demande_prevue_mensuelle
    from classification as c
    inner join demande_fenetre as f
        on c.company_id = f.company_id and c.product_id = f.product_id
    inner join demande_n1_saison as n1
        on c.company_id = n1.company_id and c.product_id = n1.product_id

)

select
    current_date('Europe/Paris') as date_calcul,
    company_id,
    product_id,
    company_code,
    company_name,
    product_code,
    product_name,
    statut_vie,
    classe_abc,
    methode_prevision,
    demande_fenetre,
    nb_mois_avec_demande,
    demande_prevue_mensuelle,
    round(demande_prevue_mensuelle / 30.4, 3) as demande_prevue_journaliere
from prevision
    );
  