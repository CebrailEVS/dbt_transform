{{ config(materialized='table') }}

-- État du parc d'équipements Nayka à chaque fin de mois, pour la valorisation de la clôture : machines, terminaux de
-- paiement et Base Cabinet présents dans un dépôt, lus dans l'historique du parc. Mêmes règles que
-- app_cockpit__nayka_equipement_stock (dépôts, catégories, propriétaire, valeur) : une évolution se reporte dans les deux.

with mois as (
    -- Fin du mois = version en vigueur à la fin du 1er jour du mois suivant, après la photo quotidienne du parc (9 h)
    -- qui suit la fin du mois. Le mois en cours prend la dernière version connue.
    select
        mois,
        timestamp(datetime(date_add(mois, interval 1 month), time(23, 59, 59)), 'Europe/Paris') as etat_at
    from
        unnest(
            generate_date_array(
                (
                    select date_trunc(min(date(dbt_valid_from, 'Europe/Paris')), month)
                    from {{ ref('snap_oracle_nayka__device') }}
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
    inner join {{ ref('snap_oracle_nayka__device') }} as s
        on
            m.etat_at >= s.dbt_valid_from
            and (s.dbt_valid_to is null or m.etat_at < s.dbt_valid_to)
    where
        s.company_code in ('DEPOTLOGISTIQUERUNGIS', 'DEPOTLOGISTIQUEDARDILLY', 'MARKETING', 'DEPOTREBUT')
        and s.device_type_id in (1, 4, 12)  -- fontaines, terminaux de paiement et Base Cabinet
),

qualifie as (
    select
        mois,
        etat_at,
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
        {{ nayka_depot_role('company_code') }} as depot_role,
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
    current_timestamp() < etat_at as is_provisoire,
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
