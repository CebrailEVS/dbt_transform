
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__nayka_equipement_fin_mois`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] \u00c9tat du parc d'\u00e9quipements Nayka \u00e0 chaque fin de mois : fontaines, Base Cabinet et terminaux de paiement pr\u00e9sents dans un d\u00e9p\u00f4t, avec leur co\u00fbt d'achat. Destin\u00e9 \u00e0 la valorisation de la cl\u00f4ture mensuelle Nayka de Cockpit Supply (machines compt\u00e9es \u00e9quipement par \u00e9quipement).\n[COMMENT CONSTRUITE] Historique du parc snap_oracle_nayka__device. Pour chaque mois couvert, l'\u00e9tat du parc \u00e0 la fin du dernier jour du mois (etat_at). La photo du parc de 9 h enregistre l'extraction Distrilog de la nuit : cet \u00e9tat n'entre dans l'historique que le 1er du mois suivant, d'o\u00f9 la lecture de la version en vigueur \u00e0 la fin de ce 1er jour (heure de Paris) ; le mois en cours prend la derni\u00e8re version connue. M\u00eames d\u00e9p\u00f4ts, types, cat\u00e9gories, propri\u00e9taire et valeurs que app_cockpit__nayka_equipement_stock.\n[GRAIN] 1 ligne par (mois, device_id).\n[NOTES] Suppose une extraction Distrilog dans la nuit du 1er : sans elle, l'\u00e9tat retenu est celui de la derni\u00e8re extraction. Pas de mois ant\u00e9rieur au d\u00e9but de l'historique du parc : rien n'est reconstitu\u00e9. Sans les lignes de commande ni les contr\u00f4les qualit\u00e9 de app_cockpit__nayka_equipement_stock ; une r\u00e8gle modifi\u00e9e dans l'un des deux mod\u00e8les se reporte dans l'autre. R\u00e9serv\u00e9 \u00e0 l'application, pas de rapport Power BI dessus.\n"""
    )
    as (
      

-- État du parc d'équipements Nayka à chaque fin de mois, pour la valorisation de la clôture : machines, terminaux de
-- paiement et Base Cabinet présents dans un dépôt, lus dans l'historique du parc. Mêmes règles que
-- app_cockpit__nayka_equipement_stock (dépôts, catégories, propriétaire, valeur) : une évolution se reporte dans les deux.

with mois as (
    -- État retenu = parc à la fin du dernier jour du mois (etat_at). La photo du parc de 9 h enregistre l'extraction
    -- Distrilog de la nuit : cet état n'entre dans l'historique que le 1er, d'où une lecture à la fin du 1er jour du mois
    -- suivant (lecture_at) ; lu à etat_at, ce serait l'état de la veille. Le mois en cours prend la dernière version.
    select
        mois,
        timestamp(datetime(last_day(mois), time(23, 59, 59)), 'Europe/Paris') as etat_at,
        timestamp(datetime(date_add(mois, interval 1 month), time(23, 59, 59)), 'Europe/Paris') as lecture_at
    from
        unnest(
            generate_date_array(
                (
                    select date_trunc(min(date(dbt_valid_from, 'Europe/Paris')), month)
                    from `evs-datastack-prod`.`snapshots`.`snap_oracle_nayka__device`
                ),
                date_trunc(current_date('Europe/Paris'), month),
                interval 1 month
            )
        ) as mois
),

parc as (
    select
        m.mois,
        m.etat_at,
        m.lecture_at,
        s.device_id,
        s.device_code,
        s.device_name,
        s.serial_number,
        s.device_type_id,
        s.parent_device_id,
        s.model_code,
        s.model_name,
        s.model_purchase_unit_price,
        s.company_code,
        s.company_name,
        s.owner_company_name,
        s.owner_company_type_id,
        s.is_active,
        s.is_reserved,
        s.device_category,
        s.device_brand,
        s.purchase_date,
        s.purchase_cost
    from mois as m
    inner join `evs-datastack-prod`.`snapshots`.`snap_oracle_nayka__device` as s
        on
            m.lecture_at >= s.dbt_valid_from
            and (s.dbt_valid_to is null or m.lecture_at < s.dbt_valid_to)
    where
        s.company_code in ('DEPOTLOGISTIQUERUNGIS', 'DEPOTLOGISTIQUEDARDILLY', 'MARKETING', 'DEPOTREBUT')
        and s.device_type_id in (1, 4, 12)  -- fontaines, terminaux de paiement et Base Cabinet
),

qualifie as (
    select
        mois,
        etat_at,
        lecture_at,
        device_id,
        device_code,
        device_name,
        model_code,
        model_name,
        device_type_id,
        case
            when device_type_id = 12 and parent_device_id is not null then 'Composant rattaché'
            when device_type_id = 12 then 'Composant en stock'
            when device_type_id = 4 then 'Terminal de paiement'
            else 'Machine principale'
        end as categorie,
        device_category as categorie_distrilog,
        serial_number,
        device_brand,
        company_code as depot_code,
        company_name as depot_name,
        case
    when company_code = 'DEPOTREBUT' then 'rebut'
    when company_code = 'MARKETING' then 'hors_stock'
    else 'stock'
end as depot_role,
        is_active,
        is_reserved,
        owner_company_name as owner_name,
        case
            when owner_company_name is null and owner_company_type_id is null then 'Non renseigné'
            when owner_company_type_id = 4 then 'Nayka'
            when owner_company_type_id = 1 then 'Fournisseur'
            when owner_company_type_id = 2 then 'Client'
            else 'Autre'
        end as owner_categorie,
        purchase_date,
        -- Un prix à 0 n'est pas distingué d'un prix non saisi à la source : il est traité comme absent.
        cast(nullif(purchase_cost, 0) as float64) as prix_achat_machine,
        cast(nullif(model_purchase_unit_price, 0) as float64) as prix_achat_modele
    from parc
)

select
    mois,
    etat_at,
    current_timestamp() < lecture_at as is_provisoire,
    device_id,
    device_code,
    device_name,
    model_code,
    model_name,
    device_type_id,
    categorie,
    categorie_distrilog,
    categorie = 'Machine principale' as is_compte_machine,
    serial_number,
    device_brand,
    depot_code,
    depot_name,
    depot_role,
    case
        when depot_role = 'rebut' then 'Rebut'
        when depot_role = 'hors_stock' then 'Hors stock (marketing)'
        when not is_active and device_type_id = 4 then 'Inactif'
        when not is_active then 'Inactif hors rebut (à investiguer)'
        when is_reserved then 'Réservé'
        else 'Disponible'
    end as statut,
    is_active,
    is_reserved,
    owner_name,
    owner_categorie,
    purchase_date,
    prix_achat_machine,
    prix_achat_modele,
    coalesce(prix_achat_machine, prix_achat_modele) as valeur,
    -- Seul le matériel propriété de la société (Nayka) entre dans les totaux de valeur (règle du Supply).
    if(owner_categorie = 'Nayka', coalesce(prix_achat_machine, prix_achat_modele), 0) as valeur_comptee,
    case
        when prix_achat_machine is not null then 'machine'
        when prix_achat_modele is not null then 'modèle'
        else 'aucun'
    end as source_prix
from qualifie
    );
  