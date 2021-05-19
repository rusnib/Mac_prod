%macro data_prep_product(
		  mpInLib = etl_ia
		, mpReportDttm = &ETL_CURRENT_DTTM.
		, mpOutCasTable = CASUSER.PRODUCT_DICTIONARY
	);

	%macro mDummy;
	%mend mDummy;

	%let lmvReportDttm = &mpReportDttm.;
	%let lmvInLib = &mpInLib.;  

	data CASUSER.PRODUCT (replace=yes drop=valid_from_dttm valid_to_dttm);
		set &lmvInLib..PRODUCT(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;
  
	data CASUSER.PRODUCT_HIERARCHY (replace=yes drop=valid_from_dttm valid_to_dttm);
		set &lmvInLib..PRODUCT_HIERARCHY(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;

	data CASUSER.product_ATTRIBUTES (replace=yes drop=valid_from_dttm valid_to_dttm);
		set &lmvInLib..product_ATTRIBUTES(where=(valid_from_dttm<=&lmvReportDttm. and valid_to_dttm>=&lmvReportDttm.));
	run;

	proc cas;
		transpose.transpose /
			table={name="PRODUCT_ATTRIBUTES", caslib="casuser", groupby={"product_id"}} 
			attributes={{name="product_id"}} 
			transpose={"PRODUCT_ATTR_VALUE"} 
			prefix="A_" 
			id={"PRODUCT_ATTR_NM"} 
			casout={name="attr_transposed", caslib="casuser", replace=true}
		;
	quit;

	proc fedsql sessref=casauto noprint;
		create table casuser.product_hier_flat{options replace=true} as
		select 
			  t1.product_id
			, t2.product_id  as LVL4_ID
			, t3.product_id  as LVL3_ID
			, t3.PARENT_product_id as LVL2_ID
			, 1 as LVL1_ID
		from 
			(select * from casuser.product_HIERARCHY where product_lvl=5) as t1
		left join 
			(select * from casuser.product_HIERARCHY where product_lvl=4) as t2
			on t1.PARENT_PRODUCT_ID = t2.PRODUCT_ID
		left join 
			(select * from casuser.product_HIERARCHY where product_lvl=3) as t3
			on t2.PARENT_PRODUCT_ID = t3.PRODUCT_ID
		;
	quit;

	proc fedsql sessref=casauto noprint;
		create table &mpOutCasTable. {options replace=true} as
		select 
			  t1.product_id
			, coalesce(t1.lvl4_id,-9999) as prod_lvl4_id
			, coalesce(t1.lvl3_id,-999) as prod_lvl3_id
			, coalesce(t1.lvl2_id,-99) as prod_lvl2_id
			, cast(1 as double) as prod_lvl1_id
			, coalesce(t15.product_nm,'NA') as product_nm
			, coalesce(t14.product_nm,'NA') as prod_lvl4_nm
			, coalesce(t13.product_nm,'NA') as prod_lvl3_nm
			, coalesce(t12.product_nm,'NA') as prod_lvl2_nm
			, cast(1 as double) as prod_lvl1_nm
			, t3.A_HERO
			, t3.A_ITEM_SIZE
			, t3.A_OFFER_TYPE
			, t3.A_PRICE_TIER
		from 
			casuser.product_hier_flat as t1
		left join 
			casuser.attr_transposed as t3
			on t1.product_id = t3.product_id
		left join 
			casuser.product as t15
			on t1.product_id = t15.product_id
		left join 
			casuser.product as t14
			on t1.lvl4_id = t14.product_id
		left join 
			casuser.product as t13
			on t1.lvl3_id = t13.product_id
		left join 
			casuser.product as t12
			on t1.lvl2_id = t12.product_id
		;
	quit;
	
	/* Clear CAS */
	proc casutil incaslib="CASUSER" ;
		droptable casdata = "attr_transposed" quiet;
		droptable casdata = "product_hier_flat" quiet;
		droptable casdata = "PRODUCT_HIERARCHY" quiet;
		droptable casdata = "PRODUCT" quiet;
		droptable casdata = "PRODUCT_ATTRIBUTES" quiet;
	run;
	
%mend data_prep_product;