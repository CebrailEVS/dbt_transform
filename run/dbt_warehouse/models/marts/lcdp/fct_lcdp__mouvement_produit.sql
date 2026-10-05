
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`fct_lcdp__mouvement_produit`
      
    partition by mouvement_date
    cluster by device_id, product_id, resources_roadman_id

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Mouvements de stock produit en machine LCDP : quantit\u00e9s charg\u00e9es, retir\u00e9es et constat\u00e9es invendues, avec leur valorisation, au grain produit \u00d7 machine \u00d7 jour \u00d7 roadman. R\u00e9pond au besoin BI \u00ab analyse produits LCDP \u00bb (chargements et invendus par r\u00e9f\u00e9rence produit, leur valorisation, et qui les a r\u00e9alis\u00e9s), sur TOUT le parc.\n[COMMENT CONSTRUITE] Tout le parc LCDP, sans filtre de type de machine : le p\u00e9rim\u00e8tre (DA FROID, DA CHAUD, full Nayax\u2026) se choisit en BI via la relation device_id \u2192 dim_lcdp__device. Seules sont \u00e9cart\u00e9es les t\u00e2ches sans machine renseign\u00e9e (retraits saisis au niveau du client, ~50 lignes, non rattachables). Deux flux agr\u00e9g\u00e9s au jour puis FULL OUTER JOIN sur (mouvement_date, device_id, product_id, roadman) : (1) chargement, depuis int_oracle_lcdp__chargement_tasks (task_type 13), r\u00e9parti par movement_type \u2014 LOADING \u2192 qty_chargee, REMOVING \u2192 qty_retiree (le sens vient du SIGNE de la quantit\u00e9, pas du label) ; (2) invendus, depuis int_oracle_lcdp__invendus_tasks (task_type 11, constat d\u00e9di\u00e9 de produits retir\u00e9s car invendus). Le FULL OUTER JOIN est n\u00e9cessaire : plus d'un tiers des constats d'invendus n'ont pas de chargement sur le m\u00eame produit \u00d7 machine \u00d7 jour. Roadman = ressource PERSON de la t\u00e2che (la plus petite si bin\u00f4me, r\u00e8gle des deux intermediates), expos\u00e9 en resources_roadman_id + code/nom aplatis depuis dim_lcdp__resource. Statuts de t\u00e2che retenus : FAIT et VALIDE des deux c\u00f4t\u00e9s \u2014 le filtre est appliqu\u00e9 ici sur le chargement, dont l'intermediate remonte aussi ANNULE et ANOMALIE, pour que qty_chargee et qty_invendus partagent la m\u00eame d\u00e9finition de \u00ab r\u00e9alis\u00e9 \u00bb. Quantit\u00e9s de retrait et d'invendus normalis\u00e9es en POSITIF. Valorisation = quantit\u00e9 \u00d7 prix d'achat courant de la fiche produit (dim_lcdp__product.purchase_unit_price via l'intermediate), m\u00e9thode identique \u00e0 fct_supply_chain__flux_neshu. Historique complet : chargements depuis 2023-12-13, invendus depuis 2024-12-11. Attributs d'affichage (device_code / device_name, product_code / product_name, roadman_code / roadman_name, company_code / company_name) aplatis depuis les dims parentes pour le confort BI ; les dims restent la source de v\u00e9rit\u00e9 et les FK sont conserv\u00e9es.\n[GRAIN] 1 ligne par product_id \u00d7 device_id \u00d7 mouvement_date \u00d7 resources_roadman_id. ~357k lignes, ~620 machines. Le roadman est NULL-able (t\u00e2che sans ressource PERSON, ~0,15 % des lignes) et fait partie du grain.\n[NOTES] TROIS MESURES DE VOLUME, JAMAIS \u00c0 CUMULER SANS D\u00c9CISION M\u00c9TIER : qty_chargee est une entr\u00e9e ; qty_retiree et qty_invendus sont deux canaux de sortie DISTINCTS et sans double comptage (retrait saisi pendant le passage de chargement vs constat d'invendus d\u00e9di\u00e9). 146 couples produit \u00d7 machine \u00d7 jour portent un chargement ET un retrait le m\u00eame jour \u2014 les sommer masquerait les deux mouvements. AUCUN RATIO N'EST CALCUL\u00c9 ICI (taux de perte, taux d'\u00e9coulement) : une mesure non additive n'a pas sa place \u00e0 ce grain, et la question \u00ab un retrait compte-t-il comme une perte ? \u00bb est un arbitrage m\u00e9tier \u2014 \u00e0 construire en BI. Attention au d\u00e9nominateur : certaines lignes portent des invendus avec qty_chargee = 0 (produit charg\u00e9 avant la fen\u00eatre, ou arriv\u00e9 par une machine hors p\u00e9rim\u00e8tre). PAS DE QUANTIT\u00c9 VENDUE PAR PRODUIT : la t\u00e9l\u00e9m\u00e9trie Nayax ne r\u00e9sout pas le produit (99,9 % des events portent product_id = 1 'INDEFINI'), la comparaison charg\u00e9 / vendu n'est donc possible qu'au niveau machine \u2014 c'est ce que fait fct_lcdp__chargement_sortie (grain device \u00d7 semaine). VALORISATION AU PRIX COURANT : une valorisation pass\u00e9e se recalcule si un tarif produit \u00e9volue, ce n'est pas un chiffre comptable fig\u00e9. Ce sont des prix d'ACHAT \u2014 les t\u00e2ches de chargement et d'invendus ne portent aucun prix de vente, aucune valorisation en manque \u00e0 gagner n'est possible depuis cette source. Un prix de fiche aberrant se propage tel quel dans les montants (constat\u00e9 : LION BROWNIE, code 30018, \u00e0 15,62 \u20ac contre une m\u00e9diane SNACK \u00e0 0,56 \u20ac). Inversement, un produit sans prix de fiche sort \u00e0 0 \u20ac et non \u00e0 NULL : 12 lignes / 28 unit\u00e9s sont ainsi valoris\u00e9es \u00e0 0 (produit 40048 SDW POULET OEUF, et le produit technique INDEFINI) \u2014 les quantit\u00e9s restent justes, les montants sont minor\u00e9s d'autant. ROADMAN : chargement et invendus sont deux t\u00e2ches distinctes, chacune avec son roadman ; sur un m\u00eame passage c'est la m\u00eame personne dans 99,4 % des cas, donc la m\u00eame ligne. Quand deux personnes interviennent le m\u00eame jour sur le m\u00eame produit \u00d7 machine, chacune a sa ligne (~30 cas) : un slicer roadman filtre ainsi les trois mesures de fa\u00e7on coh\u00e9rente. Roadman OBSERV\u00c9 (qui a fait la t\u00e2che), \u00e0 ne pas confondre avec le roadman AFFECT\u00c9 \u00e0 la machine (dim_lcdp__device). En bin\u00f4me, seul le plus petit idresources est retenu. P\u00c9RIM\u00c8TRE EN BI : aucun filtre machine ici \u2014 un visuel sans filtre additionne DA FROID, DA CHAUD, SEMI-AUTO\u2026 Filtrer chaque page sur dim_lcdp__device. Le filtre porte sur l'\u00e9tat COURANT de la machine (une machine requalifi\u00e9e d\u00e9place tout son historique). ~6 machines n'ont pas de cat\u00e9gorie dans l'ERP (label CATMACH manquant, ex. M8058 POINT P CANEJAN) et tombent en \u00ab (vide) \u00bb : exclues sans bruit d'une page filtr\u00e9e sur une cat\u00e9gorie. Le taux d'\u00e9coulement (comparaison aux ventes) reste dans fct_lcdp__chargement_sortie, limit\u00e9 au full Nayax. Le filtre \u00ab produits vendables \u00bb de chargement_sortie n'est PAS repris : \u00e0 filtrer en BI via dim_lcdp__product.product_group si besoin. CONVENTION MESURES : qty_ = quantit\u00e9 physique en unit\u00e9s de base ; montant_*_eur = valorisation au prix d'achat courant.\n"""
    )
    as (
      

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
    );
  