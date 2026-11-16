
  
    

    create or replace table `evs-datastack-prod`.`prod_app_compta`.`app_compta__section_analytique`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Sections analytiques de Sage o\u00f9 une d\u00e9pense peut \u00eatre imput\u00e9e : la liste des codes que comptapp propose quand la compta code \u00e0 la main une d\u00e9pense de v\u00e9hicule rest\u00e9e sans code.\n[COMMENT CONSTRUITE] Depuis stg_mssql_sage__f_comptea, sans intermediate : les sections qui ne sont pas en sommeil, avec leur intitul\u00e9 sans espaces de bord.\n[GRAIN] 1 ligne par section analytique (code_analytique).\n[NOTES] Lu par l'\u00e9cran \u00ab Factures fournisseurs \u00bb : le champ de saisie du code d'une d\u00e9pense \u00ab \u00c0 cat\u00e9goriser \u00bb propose ces sections et refuse tout autre code. Une section cr\u00e9\u00e9e dans Sage y arrive au chargement suivant de Sage. R\u00e9serv\u00e9 \u00e0 l'application : ne pas brancher de rapport Power BI dessus.\n"""
    )
    as (
      

-- Codes que comptapp accepte quand la compta code une dépense à la main : une section en
-- sommeil est inactive dans Sage, elle n'est donc pas proposée.
select
    ca_num as code_analytique,
    trim(ca_intitule) as intitule
from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_comptea`
where ca_sommeil = 0
    );
  