
  
    

    create or replace table `evs-datastack-prod`.`prod_app_compta`.`app_compta__vehicule_imputation`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Code analytique \u00e0 imputer \u00e0 une d\u00e9pense de v\u00e9hicule (carburant, p\u00e9age, loyer) selon la plaque et le jour : celui du collaborateur que GAC affecte au v\u00e9hicule ce jour-l\u00e0, avec son code de ce jour-l\u00e0. Sans code, le motif dit pourquoi la d\u00e9pense est \u00e0 cat\u00e9goriser.\n[COMMENT CONSTRUITE] Depuis stg_gac__historique_affectation, sans intermediate. 1) P\u00e9riodes : derni\u00e8re version de chaque \u00e9v\u00e8nement (contrat \u00d7 date d'effet) ; le fichier le plus r\u00e9cent du contrat fait foi sur la p\u00e9riode qu'il couvre, les \u00e9v\u00e8nements sortis de la fen\u00eatre de 12 mois gardent leur derni\u00e8re version. \u00c9cart\u00e9s : contrats sans plaque, \u00e9v\u00e8nements dont la fin pr\u00e9c\u00e8de l'effet. 2) Codes : fonction_actuelle non vide de chaque fichier quotidien, regroup\u00e9e en intervalles de validit\u00e9 par collaborateur. 3) Chaque p\u00e9riode est d\u00e9coup\u00e9e aux changements de code de son collaborateur.\n[GRAIN] 1 ligne par (immatriculation, date_debut) : un intervalle de jours o\u00f9 le collaborateur et son code ne changent pas. Les intervalles d'une plaque ne se chevauchent pas.\n[NOTES] Lecture par l'app : la ligne dont l'intervalle contient le jour de la d\u00e9pense ; aucune ligne = plaque inconnue de GAC ou jour hors historique. Un v\u00e9hicule en maintenance qui garde son collaborateur reste imput\u00e9 \u00e0 ce collaborateur. Un code vide dans GAC est un oubli de saisie : le code connu le plus proche s'applique, si bien qu'une fiche compl\u00e9t\u00e9e apr\u00e8s coup corrige aussi les d\u00e9penses pass\u00e9es. Avant le premier fichier charg\u00e9, le code d'un collaborateur est le plus ancien connu. R\u00e9serv\u00e9 \u00e0 l'application : ne pas brancher de rapport Power BI dessus.\n"""
    )
    as (
      

with historique as (
    select * from `evs-datastack-prod`.`prod_staging`.`stg_gac__historique_affectation`
),

-- 1. Périodes d'affectation : la dernière version de chaque évènement (contrat × date d'effet).
-- Le fichier le plus récent où figure le contrat fait foi sur toute la période qu'il couvre :
-- un évènement corrigé ou supprimé depuis n'y est plus, son ancienne version est écartée.
dernier_fichier_du_contrat as (
    select
        contrat_id_gac,
        snapshot_date,
        min(date_d_effet) as debut_couverture,
        -- La plaque vaut pour toutes les périodes du contrat : on prend la plus récente.
        max(contrat_immatriculation_edi) as contrat_immatriculation_edi
    from historique
    group by contrat_id_gac, snapshot_date
    qualify snapshot_date = max(snapshot_date) over (partition by contrat_id_gac)
),

derniere_version as (
    select *
    from historique
    qualify row_number() over (
        partition by contrat_id_gac, date_d_effet
        order by snapshot_date desc
    ) = 1
),

periodes as (
    select
        v.contrat_id_gac,
        d.contrat_immatriculation_edi,
        v.date_d_effet as date_debut,
        v.date_de_fin as date_fin,
        v.disponibilite_du_vehicule,
        v.field_colleague_id,
        v.collaborateur,
        v.email
    from derniere_version as v
    inner join dernier_fichier_du_contrat as d
        on v.contrat_id_gac = d.contrat_id_gac
    where
        (
            v.snapshot_date = d.snapshot_date
            -- sorti de la fenêtre glissante de GAC avant ce fichier : gardé dans sa dernière version
            or v.date_d_effet < d.debut_couverture
        )
        -- Sans plaque (véhicule pas encore livré), aucune facture ne peut s'y rattacher.
        and d.contrat_immatriculation_edi is not null
        -- GAC laisse passer des évènements dont la fin précède l'effet : ils ne couvrent aucun jour.
        and (v.date_de_fin is null or v.date_de_fin >= v.date_d_effet)
),

-- 2. Code analytique de chaque collaborateur dans le temps. GAC n'exporte que le code ACTUEL
-- (fonction_actuelle), recopié sur tous ses évènements : son historique se lit dans les
-- fichiers quotidiens empilés, un code par journée. Un code vide est un oubli de saisie et ne
-- fait pas foi : le code connu le plus proche s'applique, pour qu'une fiche complétée après
-- coup corrige aussi les dépenses passées.
code_par_fichier as (
    select distinct
        field_colleague_id,
        snapshot_date,
        fonction_actuelle as code_analytique
    from historique
    where field_colleague_id is not null and fonction_actuelle is not null
),

changements_de_code as (
    select
        *,
        row_number() over (partition by field_colleague_id order by snapshot_date) = 1
        or code_analytique is distinct from lag(code_analytique) over (
            partition by field_colleague_id order by snapshot_date
        ) as is_changement
    from code_par_fichier
),

sequences_de_code as (
    select
        field_colleague_id,
        code_analytique,
        snapshot_date,
        countif(is_changement) over (
            partition by field_colleague_id order by snapshot_date
        ) as numero_sequence
    from changements_de_code
),

intervalles_de_code as (
    select
        field_colleague_id,
        numero_sequence,
        any_value(code_analytique) as code_analytique,
        min(snapshot_date) as premier_snapshot_date
    from sequences_de_code
    group by field_colleague_id, numero_sequence
),

codes as (
    select
        field_colleague_id,
        code_analytique,
        -- Avant le premier fichier, le code le plus ancien connu fait foi : borne ouverte.
        if(numero_sequence = 1, null, premier_snapshot_date) as date_debut_validite,
        date_sub(
            lead(premier_snapshot_date) over (partition by field_colleague_id order by numero_sequence),
            interval 1 day
        ) as date_fin_validite
    from intervalles_de_code
),

-- 3. Une période se découpe aux changements de code de son collaborateur. Les intervalles de
-- code d'un collaborateur couvrent tout le calendrier : une période trouve au moins un code si
-- son collaborateur en a jamais eu un, sinon elle reste entière, sans code.
decoupees as (
    select
        p.contrat_immatriculation_edi as immatriculation,
        p.contrat_id_gac,
        greatest(p.date_debut, coalesce(c.date_debut_validite, p.date_debut)) as date_debut,
        case
            when p.date_fin is null then c.date_fin_validite
            when c.date_fin_validite is null then p.date_fin
            else least(p.date_fin, c.date_fin_validite)
        end as date_fin,
        p.disponibilite_du_vehicule as disponibilite,
        p.field_colleague_id as collaborateur_id,
        p.collaborateur,
        p.email,
        c.code_analytique
    from periodes as p
    left join codes as c
        on
            p.field_colleague_id = c.field_colleague_id
            and (c.date_debut_validite is null or p.date_fin is null or p.date_fin >= c.date_debut_validite)
            and (c.date_fin_validite is null or p.date_debut <= c.date_fin_validite)
)

select
    immatriculation,
    date_debut,
    date_fin,
    contrat_id_gac,
    disponibilite,
    collaborateur_id,
    collaborateur,
    email,
    code_analytique,
    case
        when collaborateur_id is null then 'Sans collaborateur'
        when code_analytique is null then 'Collaborateur sans code'
    end as motif_a_categoriser
from decoupees
    );
  