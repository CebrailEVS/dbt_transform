
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`fct_lcdp__mouvement_produit`
      
    partition by mouvement_date
    cluster by device_id, product_id

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Mouvements de stock produit en machine LCDP : quantit\u00e9s charg\u00e9es, retir\u00e9es et constat\u00e9es invendues, avec leur valorisation, au grain produit \u00d7 machine \u00d7 jour. R\u00e9pond au besoin BI \u00ab analyse produits LCDP \u00bb (chargements et invendus par r\u00e9f\u00e9rence produit, et leur valorisation).\n[COMMENT CONSTRUITE] P\u00e9rim\u00e8tre dim_lcdp__device.audit_type='1- AUDIT TELEMETRIE (NAYAX)' AND device_category='DA FROID' AND currency_mode='SANS MONNAIE' (full Nayax) \u2014 m\u00eame parc que fct_lcdp__chargement_sortie. Deux flux agr\u00e9g\u00e9s au jour puis FULL OUTER JOIN sur (mouvement_date, device_id, product_id) : (1) chargement, depuis int_oracle_lcdp__chargement_tasks (task_type 13), r\u00e9parti par movement_type \u2014 LOADING \u2192 qty_chargee, REMOVING \u2192 qty_retiree (le sens vient du SIGNE de la quantit\u00e9, pas du label) ; (2) invendus, depuis int_oracle_lcdp__invendus_tasks (task_type 11, constat d\u00e9di\u00e9 de produits retir\u00e9s car invendus). Le FULL OUTER JOIN est n\u00e9cessaire : 1 190 des 3 195 constats d'invendus (37 %) n'ont pas de chargement sur le m\u00eame produit \u00d7 machine \u00d7 jour. Statuts de t\u00e2che retenus : FAIT et VALIDE des deux c\u00f4t\u00e9s \u2014 le filtre est appliqu\u00e9 ici sur le chargement, dont l'intermediate remonte aussi ANNULE et ANOMALIE, pour que qty_chargee et qty_invendus partagent la m\u00eame d\u00e9finition de \u00ab r\u00e9alis\u00e9 \u00bb. Quantit\u00e9s de retrait et d'invendus normalis\u00e9es en POSITIF. Valorisation = quantit\u00e9 \u00d7 prix d'achat courant de la fiche produit (dim_lcdp__product.purchase_unit_price via l'intermediate), m\u00e9thode identique \u00e0 fct_supply_chain__flux_neshu. Historique complet du p\u00e9rim\u00e8tre, depuis 2024-12-11. Attributs d'affichage (device_code / device_name, product_code / product_name, company_code / company_name) aplatis depuis les dims parentes pour le confort BI ; les dims restent la source de v\u00e9rit\u00e9 et les FK sont conserv\u00e9es.\n[GRAIN] 1 ligne par product_id \u00d7 device_id \u00d7 mouvement_date. ~173k lignes.\n[NOTES] TROIS MESURES DE VOLUME, JAMAIS \u00c0 CUMULER SANS D\u00c9CISION M\u00c9TIER : qty_chargee est une entr\u00e9e ; qty_retiree et qty_invendus sont deux canaux de sortie DISTINCTS et sans double comptage (retrait saisi pendant le passage de chargement vs constat d'invendus d\u00e9di\u00e9). 146 couples produit \u00d7 machine \u00d7 jour portent un chargement ET un retrait le m\u00eame jour \u2014 les sommer masquerait les deux mouvements. AUCUN RATIO N'EST CALCUL\u00c9 ICI (taux de perte, taux d'\u00e9coulement) : une mesure non additive n'a pas sa place \u00e0 ce grain, et la question \u00ab un retrait compte-t-il comme une perte ? \u00bb est un arbitrage m\u00e9tier \u2014 \u00e0 construire en BI. Attention au d\u00e9nominateur : certaines lignes portent des invendus avec qty_chargee = 0 (produit charg\u00e9 avant la fen\u00eatre, ou arriv\u00e9 par une machine hors p\u00e9rim\u00e8tre). PAS DE QUANTIT\u00c9 VENDUE PAR PRODUIT : la t\u00e9l\u00e9m\u00e9trie Nayax ne r\u00e9sout pas le produit (99,9 % des events portent product_id = 1 'INDEFINI'), la comparaison charg\u00e9 / vendu n'est donc possible qu'au niveau machine \u2014 c'est ce que fait fct_lcdp__chargement_sortie (grain device \u00d7 semaine). VALORISATION AU PRIX COURANT : une valorisation pass\u00e9e se recalcule si un tarif produit \u00e9volue, ce n'est pas un chiffre comptable fig\u00e9. Ce sont des prix d'ACHAT \u2014 les t\u00e2ches de chargement et d'invendus ne portent aucun prix de vente, aucune valorisation en manque \u00e0 gagner n'est possible depuis cette source. Un prix de fiche aberrant se propage tel quel dans les montants (constat\u00e9 : LION BROWNIE, code 30018, \u00e0 15,62 \u20ac contre une m\u00e9diane SNACK \u00e0 0,56 \u20ac). Inversement, un produit sans prix de fiche sort \u00e0 0 \u20ac et non \u00e0 NULL : 12 lignes / 28 unit\u00e9s sont ainsi valoris\u00e9es \u00e0 0 (produit 40048 SDW POULET OEUF, et le produit technique INDEFINI) \u2014 les quantit\u00e9s restent justes, les montants sont minor\u00e9s d'autant. P\u00c9RIM\u00c8TRE : les DA FROID \u00e0 monnayeur sont exclues pour rester align\u00e9 sur fct_lcdp__chargement_sortie, alors que leur exclusion n'a pas de justification au grain produit \u2014 elle sert \u00e0 fiabiliser la comparaison aux ventes Nayax, absente ici. Elles repr\u00e9sentent +52 % de volume charg\u00e9 et +81 % d'invendus sur les DA FROID, \u00e0 rouvrir si le besoin BI s'\u00e9largit. Le filtre \u00ab produits vendables \u00bb de chargement_sortie n'est PAS repris : il ne retirerait que 7 lignes sur ce parc. CONVENTION MESURES : qty_ = quantit\u00e9 physique en unit\u00e9s de base ; montant_*_eur = valorisation au prix d'achat courant.\n"""
    )
    as (
      

-- Mouvements de stock produit en machine (LCDP), au grain produit × machine × jour.
-- Trois mesures de volume exposées séparément : une entrée (qty_chargee) et DEUX
-- canaux de sortie distincts, sans double comptage — qty_retiree (produit ressorti
-- pendant le passage de chargement, saisi en négatif : le sens vient du SIGNE, cf.
-- movement_type en amont) et qty_invendus (constat dédié, task_type 11).
-- Détail du périmètre, de la valorisation et des pièges de lecture : voir la
-- description YAML du modèle, qui fait foi.

with devices_perimeter as (
    select
        device_id,
        device_code,
        device_name
    from `evs-datastack-prod`.`prod_marts`.`dim_lcdp__device`
    where
        audit_type = '1- AUDIT TELEMETRIE (NAYAX)'
        and device_category = 'DA FROID'
        and currency_mode = 'SANS MONNAIE'
),

chargement_daily as (
    select
        date(c.task_start_date) as mouvement_date,
        c.device_id,
        c.product_id,
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
    inner join devices_perimeter as dp on c.device_id = dp.device_id
    -- Même définition de « réalisé » que les invendus en amont, qui ne retiennent
    -- que FAIT / VALIDE : sans ce filtre, qty_chargee inclurait les tâches ANNULE
    -- et ANOMALIE alors que qty_invendus les exclut déjà.
    where c.task_status_code in ('FAIT', 'VALIDE')
    group by 1, 2, 3
),

invendus_daily as (
    select
        date(i.task_start_date) as mouvement_date,
        i.device_id,
        i.product_id,
        max(i.company_id) as company_id,
        sum(i.quantity) as qty_invendus,
        sum(i.valuation) as montant_invendus_eur,
        max(i.updated_at) as updated_at
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_lcdp__invendus_tasks` as i
    inner join devices_perimeter as dp on i.device_id = dp.device_id
    group by 1, 2, 3
),

-- FULL OUTER JOIN obligatoire : 37 % des constats d'invendus n'ont pas de
-- chargement sur le même produit × machine × jour. Un LEFT JOIN sur le
-- chargement en perdrait plus d'un tiers.
mouvements as (
    select
        coalesce(c.mouvement_date, i.mouvement_date) as mouvement_date,
        coalesce(c.device_id, i.device_id) as device_id,
        coalesce(c.product_id, i.product_id) as product_id,
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
)

select
    m.mouvement_date,
    m.device_id,
    m.product_id,
    m.company_id,

    -- Attributs d'affichage aplatis depuis les dims parentes (code + libellé
    -- seulement) : évitent une jointure côté BI sans dupliquer les dimensions.
    -- company_code / company_name viennent de dim_lcdp__company via le client
    -- porté par la TÂCHE, pas du client courant de la machine aplati sur
    -- dim_lcdp__device — une machine peut changer de client dans le temps.
    dp.device_code,
    dp.device_name,
    p.product_code,
    p.product_name,
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
left join devices_perimeter as dp on m.device_id = dp.device_id
left join `evs-datastack-prod`.`prod_marts`.`dim_lcdp__product` as p on m.product_id = p.product_id
left join `evs-datastack-prod`.`prod_marts`.`dim_lcdp__company` as co on m.company_id = co.company_id
    );
  