
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`fct_commerce__activite`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER]\nFaits des activit\u00e9s commerciales Nespresso : rendez-vous, appels\nt\u00e9l\u00e9phoniques, t\u00e2ches et e-mails saisis par les commerciaux dans C4C\npour suivre la relation client / prospect.\n\n[COMMENT CONSTRUITE]\nConstruit \u00e0 partir de `stg_nesp_co__activite` (fichier Excel\n`nespresso_activites`). Unification des champs selon le type d'activit\u00e9\n(`Phone Call` / `Appointment` / `Activity Task`) : cr\u00e9ateur, nom et\ndate de d\u00e9but sont s\u00e9lectionn\u00e9s via CASE selon le type. Traduction en\nfran\u00e7ais des types, r\u00f4les, statuts et cat\u00e9gories m\u00e9tier (R1\nD\u00e9couverte, R2 Animation, R3 N\u00e9gociation, R4 Signature, R5\nFid\u00e9lisation, etc.). Filtre des lignes totalement vides (id/compte/\nnom/date tous NULL).\n\n[GRAIN]\n1 ligne par activit\u00e9 (`act_id`).\n\n[NOTES]\nSource : fichier Excel daily nesp_co (rafra\u00eechi 08:00 weekdays). Lien\nclient : `act_id_nessoft` (format `FR_<nessoft>`) ou `act_compte_id`\n(ID C4C). Pas de FK directe vers `dim_commerce__client` (s\u00e9mantique\nconserv\u00e9e pour compatibilit\u00e9 BI). Contient des dates futures pour les\nRDV programm\u00e9s.\n"""
    )
    as (
      

select * from `evs-datastack-prod`.`prod_intermediate`.`int_nesp_co__activites`
    );
  