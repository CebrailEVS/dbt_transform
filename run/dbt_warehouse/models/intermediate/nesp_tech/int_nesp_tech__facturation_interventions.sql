
  
    

    create or replace table `evs-datastack-prod`.`prod_intermediate`.`int_nesp_tech__facturation_interventions`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Cl\u00e9 et tarif de facturation par intervention technique Nespresso, d\u00e9termin\u00e9s par la typologie d'intervention, le type de machine et la zone (montagne ou non).\n[COMMENT CONSTRUITE] int_nesp_tech__interventions_dedup (\u00e9tats facturables) enrichi du flag mini-pr\u00e9ventive (jointure int_nesp_tech__articles_dedup sur code_article = 'miniprev'), machine normalis\u00e9e (ref_nesp_tech__machines_clean), type d'intervention (ref_nesp_tech__key_type_inter), zone montagne (ref_nesp_tech__dpts_montagne_factu). La cl\u00e9 key_factu est rapproch\u00e9e du tarif via ref_nesp_tech__key_facturation. Une intervention est gard\u00e9e sur sa meilleure correspondance de cl\u00e9.\n[GRAIN] 1 ligne par n_planning.\n[NOTES] P\u00e9rim\u00e8tre : \u00e9tats 'termin\u00e9e sign\u00e9e', 'signature diff\u00e9r\u00e9e', 'mise en \u00e9chec'. categorie_machine / machine_clean = 'UNDEFINED' quand la machine n'est pas mapp\u00e9e.\n"""
    )
    as (
      

with inter as (
    select
        intv.*,
        (art.code_article is not null) as mini_prev_bool,
        cast(
            substr(
                lpad(cast(intv.code_postal_site as string), 5, '0'),
                1,
                2
            ) as int64
        ) as dpt_site
    from `evs-datastack-prod`.`prod_intermediate`.`int_nesp_tech__interventions_dedup` as intv
    left join `evs-datastack-prod`.`prod_intermediate`.`int_nesp_tech__articles_dedup` as art
        on
            intv.n_planning = art.n_planning
            and art.code_article = 'miniprev'
    where
        intv.etat_intervention in ('terminée signée', 'signature différée', 'mise en échec')
),

machines_clean as (
    select
        lower(nom_machine) as nom_machine,
        categorie_machine,
        machine_clean
    from `evs-datastack-prod`.`prod_reference`.`ref_nesp_tech__machines_clean`
),

departements_montagne as (
    select cast(dpt as int64) as dpt
    from `evs-datastack-prod`.`prod_reference`.`ref_nesp_tech__dpts_montagne_factu`
    where montagne = 1
),

ref_type_inter as (
    select
        cast(type_code as string) as type_code,
        cast(repair_code_1 as string) as repair_code_1,
        cast(failure_code as string) as failure_code,
        mini_prev_bool,
        type_machine,
        type_inter_libelle
    from `evs-datastack-prod`.`prod_reference`.`ref_nesp_tech__key_type_inter`
),

joined as (
    select
        ma.machine_clean,
        r.type_inter_libelle,
        r.type_code,
        ma.categorie_machine,
        if(dm.dpt is not null, ' - Montagne', '') as montagne_factu,
        concat(
            r.type_code,
            ' - ',
            r.type_inter_libelle,
            ' - ',
            ma.machine_clean,
            if(dm.dpt is not null, ' - Montagne', '')
        ) as key_factu,
        i.n_planning,
        i.etat_intervention,
        i.intervention_type,
        i.code_machine,
        i.nom_machine,
        i.repair_code_1,
        i.failure_code,
        i.mini_prev_bool,
        i.dpt_site,
        i.date_heure_fin,

        row_number() over (
            partition by i.n_planning
            order by
                (case when r.repair_code_1 is not null then 16 else 0 end)
                + (case when r.failure_code is not null then 8 else 0 end)
                + (case when r.mini_prev_bool is not null then 4 else 0 end)
                + (case when r.type_machine is not null then 2 else 0 end)
                desc
        ) as rn

    from inter as i
    left join machines_clean as ma
        on lower(i.nom_machine) = ma.nom_machine
    left join ref_type_inter as r
        on
            i.intervention_type = r.type_code
            and (i.repair_code_1 = r.repair_code_1 or r.repair_code_1 is null)
            and (i.failure_code = r.failure_code or r.failure_code is null)
            and (i.mini_prev_bool = r.mini_prev_bool or r.mini_prev_bool is null)
            and (ma.categorie_machine = r.type_machine or r.type_machine is null)
    left join departements_montagne as dm
        on i.dpt_site = dm.dpt
),

final as (
    select
        j.n_planning,
        j.etat_intervention,
        j.categorie_machine,
        j.machine_clean,
        j.type_inter_libelle,
        j.key_factu,
        kf.prod_factu,
        kf.tarif_factu
    from joined as j
    left join `evs-datastack-prod`.`prod_reference`.`ref_nesp_tech__key_facturation` as kf
        on j.key_factu = kf.key_ref_inter
    where j.rn = 1
)

select
    n_planning,
    etat_intervention,
    coalesce(categorie_machine, 'UNDEFINED') as categorie_machine,
    coalesce(machine_clean, 'UNDEFINED') as machine_clean,
    type_inter_libelle,
    key_factu,
    prod_factu,
    tarif_factu

from final
    );
  