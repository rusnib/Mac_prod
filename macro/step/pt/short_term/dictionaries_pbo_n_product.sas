proc casutil;
	load data=IA.ia_pbo_loc_hierarchy casout='ia_pbo_loc_hierarchy' outcaslib='casuser' replace;
	load data=IA.ia_product_hierarchy casout='ia_product_hierarchy' outcaslib='casuser' replace;
quit;
proc fedsql sessref=casauto;
	create table casuser.pbo_hier_flat{options replace=true} as
		select
			t1.pbo_location_id, 
			t2.PBO_LOCATION_ID as LVL3_ID,
			t2.PARENT_PBO_LOCATION_ID as LVL2_ID, 
			1 as LVL1_ID
		from 
			(select * from casuser.ia_pbo_loc_hierarchy where pbo_location_lvl=4) as t1
		left join 
			(select * from casuser.ia_pbo_loc_hierarchy where pbo_location_lvl=3) as t2
		on t1.PARENT_PBO_LOCATION_ID=t2.PBO_LOCATION_ID
	;
	create table casuser.lvl4{options replace=true} as 
		select distinct
			pbo_location_id as pbo_location_id,
			pbo_location_id as pbo_leaf_id
		from
			casuser.pbo_hier_flat
	;
	create table casuser.lvl3{options replace=true} as 
		select distinct
			LVL3_ID as pbo_location_id,
			pbo_location_id as pbo_leaf_id
		from
			casuser.pbo_hier_flat
	;
	create table casuser.lvl2{options replace=true} as 
		select distinct
			LVL2_ID as pbo_location_id,
			pbo_location_id as pbo_leaf_id
		from
			casuser.pbo_hier_flat
	;
	create table casuser.lvl1{options replace=true} as 
		select 
			1 as pbo_location_id,
			pbo_location_id as pbo_leaf_id
		from
			casuser.pbo_hier_flat
	;
quit;
/* Соединяем в единый справочник ПБО */
data casuser.pbo_lvl_all;
	set casuser.lvl4 casuser.lvl3 casuser.lvl2 casuser.lvl1;
run;

/* ***************************************************** */
/* ***************************************************** */
/* ***************************************************** */
/* ***************************************************** */
/* ***************************************************** */
/* ***************************************************** */
/* ***************************************************** */


proc fedsql sessref=casauto;
   create table casuser.product_hier_flat{options replace=true} as
		select t1.product_id, 
			   t2.product_id  as LVL4_ID,
			   t3.product_id  as LVL3_ID,
			   t3.PARENT_product_id as LVL2_ID, 
			   1 as LVL1_ID
		from 
		(select * from casuser.ia_product_hierarchy where product_lvl=5) as t1
		left join 
		(select * from casuser.ia_product_hierarchy where product_lvl=4) as t2
		on t1.PARENT_PRODUCT_ID=t2.PRODUCT_ID
		left join 
		(select * from casuser.ia_product_hierarchy where product_lvl=3) as t3
		on t2.PARENT_PRODUCT_ID=t3.PRODUCT_ID
 	;
	create table casuser.lvl5{options replace=true} as 
		select distinct
			product_id as product_id,
			product_id as product_leaf_id
		from
			casuser.product_hier_flat
	;
	create table casuser.lvl4{options replace=true} as 
		select distinct
			LVL4_ID as product_id,
			product_id as product_leaf_id
		from
			casuser.product_hier_flat
	;
	create table casuser.lvl3{options replace=true} as 
		select distinct
			LVL3_ID as product_id,
			product_id as product_leaf_id
		from
			casuser.product_hier_flat
	;
	create table casuser.lvl2{options replace=true} as 
		select distinct
			LVL2_ID as product_id,
			product_id as product_leaf_id
		from
			casuser.product_hier_flat
	;
	create table casuser.lvl1{options replace=true} as 
		select 
			1 as product_id,
			product_id as product_leaf_id
		from
			casuser.product_hier_flat
	;
quit;
/* Соединяем в единый справочник  */
data casuser.product_lvl_all;
	set casuser.lvl5 casuser.lvl4 casuser.lvl3 casuser.lvl2 casuser.lvl1;
run;