-- Exemples fictifs des formes saisies dans Distrilog : la macro doit rendre le n° attendu, et rien pour une
-- immatriculation, un mot qui contient « es » ou un n° seul hors TAMPON.
-- depends_on: `evs-datastack-prod`.`prod_intermediate`.`int_oracle_lcdp__livraison_interne_tasks`
with exemples as (
    select *
    from unnest([
        struct('ES 1234 – CLIENT A' as commentaire, 'TAMPON' as destination, '1234' as attendu),
        struct('ES-1235', 'TAMPON', '1235'),
        struct('ES- 1236 CLIENT B', 'TAMPON', '1236'),
        struct('es 1237', 'TAMPON', '1237'),
        struct('ES  1238', 'TAMPON', '1238'),
        struct('ES1243', 'TAMPON', '1243'),
        struct('ES 1086/188368', 'TAMPON', '1086'),
        struct('n°AB15 17/02/2026 14:29 / ES 1258 CLIENT C', 'TAMPON', '1258'),
        struct('ES 0957', 'TAMPON', '957'),
        struct('980', 'TAMPON', '980'),
        struct('980', 'LOG', cast(null as string)),
        struct('ES-164-ZL', 'TAMPON', cast(null as string)),
        struct('ESSAI 1241', 'TAMPON', cast(null as string)),
        struct('DES 1242', 'TAMPON', cast(null as string)),
        struct('POUR BOUTIQUE', 'TAMPON', cast(null as string)),
        struct(cast(null as string), 'TAMPON', cast(null as string))
    ])
)

select
    commentaire,
    destination,
    attendu,
    cast(cast(coalesce(
    regexp_extract(
        regexp_replace(commentaire, r'(?i)\b[a-z]{2}-\d{3}-[a-z]{2}\b', ' '),
        r'(?i)\bes\s*[-–—_./:#°]?\s*(\d{3,6})\b'
    ),
    if(destination = 'TAMPON', regexp_extract(commentaire, r'^\s*(\d{3,6})\s*$'), null)
) as int64) as string) as obtenu
from exemples
where cast(cast(coalesce(
    regexp_extract(
        regexp_replace(commentaire, r'(?i)\b[a-z]{2}-\d{3}-[a-z]{2}\b', ' '),
        r'(?i)\bes\s*[-–—_./:#°]?\s*(\d{3,6})\b'
    ),
    if(destination = 'TAMPON', regexp_extract(commentaire, r'^\s*(\d{3,6})\s*$'), null)
) as int64) as string) is distinct from attendu