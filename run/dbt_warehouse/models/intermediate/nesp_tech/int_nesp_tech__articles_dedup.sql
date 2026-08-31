
  
    

    create or replace table `evs-datastack-prod`.`prod_intermediate`.`int_nesp_tech__articles_dedup`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Articles (pi\u00e8ces d\u00e9tach\u00e9es, consommables) pos\u00e9s ou utilis\u00e9s lors des interventions techniques Nespresso.\n[COMMENT CONSTRUITE] stg_nesp_tech__articles d\u00e9dupliqu\u00e9 par (n_planning, code_article), en gardant la ligne au extracted_at le plus r\u00e9cent.\n[GRAIN] 1 ligne par (n_planning, code_article).\n[NOTES] code_article = 'miniprev' est utilis\u00e9 par int_nesp_tech__facturation_interventions pour flagguer les mini-pr\u00e9ventives (mini_prev_bool).\n"""
    )
    as (
      

with ranked as (

    select *
    from `evs-datastack-prod`.`prod_staging`.`stg_nesp_tech__articles`

    qualify ROW_NUMBER() over (
        partition by n_planning, code_article
        order by extracted_at desc
    ) = 1

)

select * from ranked
    );
  