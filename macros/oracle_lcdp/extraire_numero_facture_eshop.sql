{#
    N° de facture e-shop (WooCommerce) saisi dans le commentaire d'une livraison interne Distrilog
    (« ES 1469 – … » → '1469') : chiffres seuls, sans zéro de tête, NULL si absent. Les
    immatriculations (« ES-123-AB ») sont neutralisées avant la recherche ; un n° saisi seul n'est
    retenu que vers TAMPON, où partent les commandes e-shop. Le commentaire n'est jamais exposé.
#}
{% macro extraire_numero_facture_eshop(commentaire, code_destination) -%}
cast(cast(coalesce(
    regexp_extract(
        regexp_replace({{ commentaire }}, r'(?i)\b[a-z]{2}-\d{3}-[a-z]{2}\b', ' '),
        r'(?i)\bes\s*[-–—_./:#°]?\s*(\d{3,6})\b'
    ),
    if({{ code_destination }} = 'TAMPON', regexp_extract({{ commentaire }}, r'^\s*(\d{3,6})\s*$'), null)
) as int64) as string)
{%- endmacro %}
