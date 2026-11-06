-- back compat for old kwarg name
  
  
        
            
            
            
            
        
    

    

    merge into `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__task_has_product` as DBT_INTERNAL_DEST
        using (
        select
        * from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__task_has_product__dbt_tmp094233847192`
        ) as DBT_INTERNAL_SOURCE
        on ((DBT_INTERNAL_SOURCE.idtask_has_product = DBT_INTERNAL_DEST.idtask_has_product))

    
    when matched then update set
        `idtask_has_product` = DBT_INTERNAL_SOURCE.`idtask_has_product`,`idtask` = DBT_INTERNAL_SOURCE.`idtask`,`idproduct` = DBT_INTERNAL_SOURCE.`idproduct`,`idproduct_source` = DBT_INTERNAL_SOURCE.`idproduct_source`,`idproduct_destination` = DBT_INTERNAL_SOURCE.`idproduct_destination`,`idtax` = DBT_INTERNAL_SOURCE.`idtax`,`idtask_has_product_associed` = DBT_INTERNAL_SOURCE.`idtask_has_product_associed`,`iddevice` = DBT_INTERNAL_SOURCE.`iddevice`,`idcompany_peer` = DBT_INTERNAL_SOURCE.`idcompany_peer`,`idlocation` = DBT_INTERNAL_SOURCE.`idlocation`,`idproduct_unit` = DBT_INTERNAL_SOURCE.`idproduct_unit`,`type_product_source` = DBT_INTERNAL_SOURCE.`type_product_source`,`type_product_destination` = DBT_INTERNAL_SOURCE.`type_product_destination`,`code` = DBT_INTERNAL_SOURCE.`code`,`name` = DBT_INTERNAL_SOURCE.`name`,`priceline_number` = DBT_INTERNAL_SOURCE.`priceline_number`,`real_quantity` = DBT_INTERNAL_SOURCE.`real_quantity`,`average_purchase_price` = DBT_INTERNAL_SOURCE.`average_purchase_price`,`tax_rate` = DBT_INTERNAL_SOURCE.`tax_rate`,`tax_amount` = DBT_INTERNAL_SOURCE.`tax_amount`,`global_discount_amount` = DBT_INTERNAL_SOURCE.`global_discount_amount`,`net_price` = DBT_INTERNAL_SOURCE.`net_price`,`sale_amount_net_tax` = DBT_INTERNAL_SOURCE.`sale_amount_net_tax`,`sale_amount_net` = DBT_INTERNAL_SOURCE.`sale_amount_net`,`sale_unit_price` = DBT_INTERNAL_SOURCE.`sale_unit_price`,`purchase_unit_price` = DBT_INTERNAL_SOURCE.`purchase_unit_price`,`sale_amount_brut` = DBT_INTERNAL_SOURCE.`sale_amount_brut`,`unit_coeff_multi` = DBT_INTERNAL_SOURCE.`unit_coeff_multi`,`sale_unit_price_tax` = DBT_INTERNAL_SOURCE.`sale_unit_price_tax`,`unit_coeff_div` = DBT_INTERNAL_SOURCE.`unit_coeff_div`,`created_at` = DBT_INTERNAL_SOURCE.`created_at`,`updated_at` = DBT_INTERNAL_SOURCE.`updated_at`,`extracted_at` = DBT_INTERNAL_SOURCE.`extracted_at`
    

    when not matched then insert
        (`idtask_has_product`, `idtask`, `idproduct`, `idproduct_source`, `idproduct_destination`, `idtax`, `idtask_has_product_associed`, `iddevice`, `idcompany_peer`, `idlocation`, `idproduct_unit`, `type_product_source`, `type_product_destination`, `code`, `name`, `priceline_number`, `real_quantity`, `average_purchase_price`, `tax_rate`, `tax_amount`, `global_discount_amount`, `net_price`, `sale_amount_net_tax`, `sale_amount_net`, `sale_unit_price`, `purchase_unit_price`, `sale_amount_brut`, `unit_coeff_multi`, `sale_unit_price_tax`, `unit_coeff_div`, `created_at`, `updated_at`, `extracted_at`)
    values
        (`idtask_has_product`, `idtask`, `idproduct`, `idproduct_source`, `idproduct_destination`, `idtax`, `idtask_has_product_associed`, `iddevice`, `idcompany_peer`, `idlocation`, `idproduct_unit`, `type_product_source`, `type_product_destination`, `code`, `name`, `priceline_number`, `real_quantity`, `average_purchase_price`, `tax_rate`, `tax_amount`, `global_discount_amount`, `net_price`, `sale_amount_net_tax`, `sale_amount_net`, `sale_unit_price`, `purchase_unit_price`, `sale_amount_brut`, `unit_coeff_multi`, `sale_unit_price_tax`, `unit_coeff_div`, `created_at`, `updated_at`, `extracted_at`)


    