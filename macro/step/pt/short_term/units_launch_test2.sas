cas casauto;
caslib _all_ assign;


%let lmvAnalTable = MAX_CASL.ALL_ML_TRAIN_DEC;

proc fedsql sessref=casauto;
	create table casuser.table_stat{options replace=true} as
	select  LVL2_ID, PROD_LVL2_ID
		, count(distinct sales_dt) as 		count_dts
		, count(distinct product_id) as 	count_sku
		, count(distinct pbo_location_id) as count_loc
		, count(sales_dt) as obs_num
		, sum(sum_qty) as sum_qty
	from &lmvAnalTable.
	where channel_cd = 1
	group by LVL2_ID, PROD_LVL2_ID
	;
quit;


%macro load_model_table(mpFile=&external_modeltable., mpModTable=&modeltable.);
	proc casutil incaslib="casuser" outcaslib="casuser";
		droptable casdata="&mpModTable." quiet;
	run;

	%let max_length = $1000;																			/* хватит длины 1000??? у interval сейчас около 670 */

	data casuser.&mpModTable.;																			
		length filter model params interval nominal &max_length.;
		infile "&mpFile." dsd firstobs=2;                 
		input filter $ model $ params $ interval $ nominal $ train score n;                            
	run;
	
/* 	proc casutil;                            */
/* 	    save casdata="&mpModTable." incaslib="models" outcaslib="models" replace;  */
/* 		promote casdata="&mpModTable." incaslib="Models" outcaslib="Models"; */
/* 	run; */
%mend load_model_table;

%load_model_table(mpFile= /data/files/input/PMIX_MODEL_TABLE.csv, mpModTable=PMIX_MODEL_TABLE);

data casuser.MODEL_LIST;
	set casuser.PMIX_MODEL_TABLE;
	lvl2_id = input(scan(filter, 3), best32.);
	prod_lvl2_id = input(scan(filter, 7), best32.);
run;

proc fedsql sessref=casauto;
	create table casuser.full_join{options replace=true} as
	select 
		  coalesce(st.LVL2_ID, ml.LVL2_ID) as LVL2_ID
		, coalesce(st.PROD_LVL2_ID, ml.PROD_LVL2_ID) as PROD_LVL2_ID
		, case 
			when ml.LVL2_ID is null 
				then 'ABT'
			when st.LVL2_ID is null 
				then 'MLT'
			else 'ALL'
		  end as mismatch_flag
		, st.count_dts
		, st.count_sku
		, st.count_loc
		, st.obs_num
		, st.sum_qty
		, ml.filter 
		, ml.model 
 		, ml.params 
 		, ml.interval 
		, ml.nominal
	from casuser.table_stat as st
	full join casuser.MODEL_LIST as ml
	on st.LVL2_ID = ml.LVL2_ID
		and st.PROD_LVL2_ID = ml.PROD_LVL2_ID
	;
quit;


data work.full_join;
set casuser.full_join;
format 
	count_dts	commax15.
	count_sku	commax15.
	count_loc	commax15.
	obs_num		commax15.
	sum_qty		commax15.
	;
run;

data casuser.check;
set casuser.full_join;
keep PROD_LVL2_ID  LVL2_ID filter mismatch_flag;
run;


libname nac "/data/MN_CALC"; 
proc sql;
	create table work.sku as 
    select distinct
        PROD_LVL2_ID, PROD_LVL2_NM
    from
        nac.product_dictionary_ml
    ;
	create table work.loc as 
    select distinct
        LVL2_ID, LVL2_NM
    from
        nac.PBO_DICTIONARY_ML
    ;
	create table work.sku_x_loc as 
    select sku.*, loc.*
    from
       	work.sku as sku
	cross join
		work.loc as loc
    ;
quit;

data casuser.sku_x_loc;
set work.sku_x_loc;
run;

proc fedsql sessref=casauto;
	create table casuser.intersect_names{options replace=true} as
	select st.*
	from casuser.sku_x_loc as st
	inner join casuser.check as ml
	on st.LVL2_ID = ml.LVL2_ID
		and st.PROD_LVL2_ID = ml.PROD_LVL2_ID
		and ml.mismatch_flag = 'MLT'
	;
quit;