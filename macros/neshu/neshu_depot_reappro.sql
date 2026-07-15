{% macro neshu_depot_reappro(company_col) %}
-- Rattachement des dépôts au pilotage du réapprovisionnement NESHU (forecast / point de commande).
--
-- Strasbourg (118) est réapprovisionné DEPUIS Lyon (116) : il ne passe quasiment aucune commande
-- fournisseur en direct (vérifié en données : 4 lignes sur tout l'historique). Sa demande et son
-- stock doivent donc remonter sur Lyon, qui commande au fournisseur pour le territoire Lyon +
-- Strasbourg puis réexpédie en interne. On rattache donc 118 → 116.
--
-- Bordeaux (120) commande de façon indépendante (Distrilog) et n'est PAS remappé : il est simplement
-- exclu du pilotage en aval (filtre sur le périmètre 114/116/364 = Rungis / Lyon(+Strasbourg) /
-- Marseille), puisqu'après remap il reste le seul id hors de ce périmètre.
--
-- Macro partagée par le socle de demande (int_oracle_neshu__demande_mensuelle) et par les CTE stock
-- et encours du point de commande, pour que le rattachement reste cohérent sur les trois flux.
case when {{ company_col }} = 118 then 116 else {{ company_col }} end
{% endmacro %}
