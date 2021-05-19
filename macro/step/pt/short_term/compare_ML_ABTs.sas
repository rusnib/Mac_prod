/* Проверка 1 */
/* Проверка на кол-во товаров-магазинов по дням */
proc fedsql sessref=casauto;
	create table casuser.my  {options replace=true}as
	select sales_dt
		, count(distinct pbo_location_id) 	as loc
		, count(distinct product_id) 		as sku
		, count(distinct pbo_location_id) 	as chan
	from MAX_CASL.UNITS_ABT_MAY_TEST
	group by sales_dt
	;
	create table casuser.nik  {options replace=true}as
	select sales_dt
		, count(distinct pbo_location_id) 	as loc
		, count(distinct product_id) 		as sku
		, count(distinct pbo_location_id) 	as chan
	from MN_SHORT.ALL_ML_TRAIN
	group by sales_dt
	;
	create table casuser.compare  {options replace=true}as
	select coalesce(my.sales_dt, nik.sales_dt) as sales_dt
		, my.loc   as my_loc  
		, my.sku   as my_sku
		, my.chan  as my_chan
		, nik.loc   as nik_loc  
		, nik.sku   as nik_sku
		, nik.chan  as nik_chan
		, sum(nik.loc  , - my.loc ) as d_loc  
		, sum(nik.sku  , - my.sku ) as d_sku
		, sum(nik.chan , - my.chan) as d_chan

	from casuser.my as my
	full join casuser.nik as nik
		on my.sales_dt = nik.sales_dt	
	;
quit;
	


/* Проверка 3 */
/* Подготовка full join витрины для VA для сравнения */
proc casutil;
  droptable casdata="join_1" incaslib="casuser" quiet;
quit; 
data MN_SHORT.ALL_ML_TEST;
set 
	MN_SHORT.ALL_ML_TRAIN_TEST
	MN_SHORT.ALL_ML_SCORING_TEST
	;
run;


proc fedsql sessref=casauto;;
create table casuser.join as
select 
	 coalesce(my.pbo_location_id , nik.pbo_location_id) as pbo_location_id
	,coalesce(my.product_id , nik.product_id) as product_id
	,coalesce(my.sales_dt , nik.sales_dt) as sales_dt  
	,my.CHANNEL_CD      
	,my.SUM_QTY        as my_SUM_QTY
	,my.GROSS_PRICE_AMT as my_GROSS_PRICE_AMT 
	,my.SUM_TRP        as my_SUM_TRP
	,my.temperature    as my_temperature
	,nik.precipitation  as my_precipitation
	,nik.SUM_QTY        as nik_SUM_QTY
	,nik.GROSS_PRICE_AMT as nik_GROSS_PRICE_AMT 
	,nik.SUM_TRP        as nik_SUM_TRP
	,nik.temperature    as nik_temperature
	,nik.precipitation  as nik_precipitation
	,my.A_CPI as my_A_CPI
	,my.A_GPD as my_A_GPD
	,my.A_RDI as my_A_RDI
	,nik.A_CPI as nik_A_CPI
	,nik.A_GPD as nik_A_GPD
	,nik.A_RDI as nik_A_RDI
	,my.COMP_TRP_KFC as my_COMP_TRP_KFC
	,my.COMP_TRP_BK as my_COMP_TRP_BK
	,nik.COMP_TRP_KFC as nik_COMP_TRP_KFC
	,nik.COMP_TRP_BK as nik_COMP_TRP_BK
from 
	MAX_CASL.UNITS_ABT_MAY_TEST as my
full join 
	MN_SHORT.ALL_ML_TEST as nik
on my.pbo_location_id   = nik.pbo_location_id
	and my.product_id        = nik.product_id
	and my.sales_dt          = nik.sales_dt
;
quit;

data casuser.join_1;
set casuser.join;
where pbo_location_id <> 29028 and  product_id <> 7458;
run;

proc casutil;
	promote casdata="join_1" casout="join_1" incaslib="casuser" outcaslib="casuser";
run;

data casuser.test_for_join_1;
set casuser.join_1;
where CHANNEL_CD is null;
run;