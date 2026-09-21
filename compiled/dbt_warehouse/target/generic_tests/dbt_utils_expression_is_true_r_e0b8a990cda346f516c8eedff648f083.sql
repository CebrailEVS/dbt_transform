



select
    1
from `evs-datastack-prod`.`prod_reference`.`ref_nesp_tech__articles_prix`

where not(article_prix_unitaire  >= 0)

