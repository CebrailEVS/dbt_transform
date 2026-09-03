
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  

-- LE VRAI GARDE-FOU DU 1:1 RAPPORT / MODELE SEMANTIQUE.
--
-- Les deux faits d'usage rattachent au rapport des rafraichissements qui sont
-- techniquement portes par le MODELE. C'est exact tant qu'un modele du parc ne
-- sert qu'un rapport. Le test `unique` sur dim_bi__rapport.dataset_id ne
-- verifie cette hypothese que DANS le parc filtre : un second rapport bati sur
-- un modele du parc mais ecarte par les filtres obligatoires (espace non
-- partage, ou rapport de metriques d'usage) passerait ce test tout en
-- invalidant l'hypothese -- et 100 % des rafraichissements du modele seraient
-- imputes au seul rapport visible, gonflant son cout.
--
-- Ce test ferme l'angle mort en repartant de l'inventaire BRUT, non filtre.
-- Les copies d'App sont d'abord rabattues sur leur original via
-- `original_report_object_id` : une copie n'est pas un second rapport, c'est le
-- meme rapport publie dans une App, et ses rafraichissements sont bien ceux du
-- rapport d'origine.
--
-- Etat au 2026-09-03 : 29 modeles du parc sont references par plusieurs
-- rapports bruts, mais ZERO apres rabattement des copies -- l'angle mort est
-- reel et vide. Si ce test se declenche, l'imputation des rafraichissements au
-- rapport n'est plus valide : il faut alors porter la mesure de cout au grain
-- du modele semantique plutot qu'a celui du rapport.

with parc as (
    select
        report_id,
        dataset_id
    from `evs-datastack-prod`.`prod_marts`.`dim_bi__rapport`
),

inventaire_brut as (
    select
        dataset_id,
        coalesce(original_report_object_id, id) as report_racine
    from `evs-datastack-prod`.`prod_raw`.`powerbi_reports`
)

select
    p.dataset_id,
    count(distinct b.report_racine) as nb_rapports_racines
from parc as p
inner join inventaire_brut as b on p.dataset_id = b.dataset_id
group by p.dataset_id
having count(distinct b.report_racine) > 1
  
  
      
    ) dbt_internal_test