cas casauto;
caslib _all_ assign;

/*таблица с ценами в разрезе товар-пбо-интервал_дат*/
proc fedsql sessref=casauto;
	select min (START_DT) from MN_DICT.PRICE_REGULAR_FUTURE;
	select max (END_DT) from MN_DICT.PRICE_REGULAR_FUTURE;
/* 	select min (START_DT) from MN_DICT.PRICE_REGULAR_PAST; */
/* 	select max (END_DT) from MN_DICT.PRICE_REGULAR_PAST; */
quit;

/* proc fedsql sessref=casauto; */
/* select count(*) from MN_DICT.PRICE_REGULAR_FUTURE */
/* where start_dt is null; */
/* select count(*) from MN_DICT.PRICE_REGULAR_FUTURE */
/* where end_dt is null; */
/* select count(*) from MN_DICT.PRICE_REGULAR_PAST */
/* where start_dt is null; */
/* select count(*) from MN_DICT.PRICE_REGULAR_PAST */
/* where end_dt is null; */
/* quit; */
/* data casuser.price_reg_past; */
/* 	set MN_DICT.PRICE_REGULAR_PAST; */
/* 	retain _past 1; */
/* 	if start_dt ne . and end_dt ne . then */
/* 	do period_dt=max(start_dt,&VF_HIST_START_DT_SAS) to min(end_dt,&VF_FC_AGG_END_DT_SAS); */
/* 	output; */
/* 	end; */
/* run; */
data casuser.price_reg_future;
	set MN_DICT.PRICE_REGULAR_FUTURE;
	retain _past 0;
	if start_dt ne . and end_dt ne . then
	do period_dt=max(start_dt,&VF_HIST_START_DT_SAS) to min(end_dt,&VF_FC_AGG_END_DT_SAS);
	output;
	end;
run;

/* check for duplicates */
/* proc fedsql sessref=casauto; */
/* 	create table casuser.p_r_p_dup{options replace=true} as */
/* 	select  */
/* 	period_dt, */
/* 	product_id, */
/* 	pbo_location_id, */
/* 	count(*) as ct */
/* 	from casuser.price_reg_past  */
/* 	group by 1,2,3 */
/* 	having count(*)>1; */
/* quit; */
/*  */
/* proc fedsql sessref=casauto; */
/* 	create table casuser.p_r_f_dup{options replace=true} as */
/* 	select  */
/* 	period_dt, */
/* 	product_id, */
/* 	pbo_location_id, */
/* 	count(*) as ct */
/* 	from casuser.price_reg_future  */
/* 	group by 1,2,3 */
/* 	having count(*)>1; */
/* quit; */

/* proc fedsql sessref=casauto; */
/* 	create table casuser.prices_flat{options replace=true} as */
/* 	select  */
/* 	coalesce(t2.period_dt,t1.period_dt) as period_dt, */
/* 	coalesce(t2.product_id,t1.product_id) as product_id, */
/* 	coalesce(t2.pbo_location_id,t1.pbo_location_id) as pbo_location_id, */
/* 	coalesce(t2.GROSS_PRICE_AMT,t1.GROSS_PRICE_AMT) as GROSS_PRICE_AMT, */
/* 	coalesce(t2.NET_PRICE_AMT,t1.NET_PRICE_AMT) as NET_PRICE_AMT */
/* 	from casuser.price_reg_past t1 full outer join casuser.price_reg_future t2 */
/* 	on t1.product_id=t2.product_id and t1.pbo_location_id=t2.pbo_location_id and */
/* 		t1.period_dt=t2.period_dt; */
/* quit; */

data casuser.prices_flat1;
	format period_dt date9. product_id pbo_location_id 32.;
	set /*casuser.price_reg_past*/ casuser.price_reg_future;
	by product_id pbo_location_id period_dt _past;
	if first.period_dt then output;
run;

proc casutil;
*droptable casdata="price_reg_past" incaslib="casuser" quiet;
*droptable casdata="price_reg_future" incaslib="casuser" quiet;
run;
/*-=-=-=-=-PROMO=-=-=-=-=*/
/* data casuser.price_promo_past; */
/* 	set MN_DICT.PRICE_promo_past; */
/* 	drop channel_cd; */
/* 	retain _past 1; */
/* 	if start_dt ne . and end_dt ne . and upcase(channel_cd)='ALL' then */
/* 	do period_dt=max(start_dt,&VF_HIST_START_DT_SAS) to min(end_dt,&VF_FC_AGG_END_DT_SAS); */
/* 	output; */
/* 	end; */
/* run; */

data casuser.price_promo_future;
	set MN_DICT.PRICE_promo_future;
	drop channel_cd;
	retain _past 0;
	if start_dt ne . and end_dt ne . and upcase(channel_cd)='ALL' then
	do period_dt=max(start_dt,&VF_HIST_START_DT_SAS) to min(end_dt,&VF_FC_AGG_END_DT_SAS);
	output;
	end;
run;

/* check for duplicates */
/* proc fedsql sessref=casauto; */
/* 	create table casuser.p_p_p_dup{options replace=true} as */
/* 	select  */
/* 	period_dt, */
/* 	product_id, */
/* 	pbo_location_id, */
/* 	promo_id, */
/* 	count(*) as ct */
/* 	from casuser.price_promo_past  */
/* 	group by 1,2,3,4 */
/* 	having count(*)>1; */
/* quit; */
/*  */
/* proc fedsql sessref=casauto; */
/* 	create table casuser.p_p_f_dup{options replace=true} as */
/* 	select  */
/* 	period_dt, */
/* 	product_id, */
/* 	pbo_location_id, */
/* 	promo_id, */
/* 	count(*) as ct */
/* 	from casuser.price_promo_future */
/* 	group by 1,2,3,4 */
/* 	having count(*)>1; */
/* quit; */

data casuser.prices_flat2;
	format period_dt date9. product_id pbo_location_id 32.;
	set /*casuser.price_promo_past*/ casuser.price_promo_future;
	by product_id pbo_location_id promo_id period_dt _past;
	if first.period_dt then output;
run;


proc casutil;
*droptable casdata="PRICE_promo_past" incaslib="casuser" quiet;
*droptable casdata="price_promo_future" incaslib="casuser" quiet;
run;


proc fedsql sessref=casauto;
create table casuser.prices_flat2_nopromo{options replace=true} as
select period_dt, product_id, pbo_location_id,
avg(GROSS_PRICE_AMT) as A_GROSS_PRICE_AMT,
avg(NET_PRICE_AMT) as A_NET_PRICE_AMT,
min(GROSS_PRICE_AMT) as M_GROSS_PRICE_AMT,
min(NET_PRICE_AMT) as M_NET_PRICE_AMT,
count(*) as promo_ct
from casuser.prices_flat2
group by 1,2,3
;
quit;


proc casutil;
*droptable casdata="PRICES_flat2" incaslib="casuser" quiet;
run;

data casuser.price_feat;
  merge casuser.prices_flat1(rename=(net_price_amt=price_reg_net gross_price_amt=price_reg_gross)) 
		casuser.prices_flat2_nopromo(rename=( M_GROSS_PRICE_AMT = price_prom_gross M_NET_PRICE_AMT=price_prom_net));
	where period_dt <= '1sep2021'd;
  by product_id pbo_location_id period_dt;

  keep product_id pbo_location_id period_dt price_reg_gross price_prom_gross
  discount_rub discount_pct promo_ct price_fact_gross price_fact_net;
  if price_prom_gross>0 then do;
	price_fact_gross=price_prom_gross ;
	price_fact_net=price_prom_net ;
    discount_rub=max(0,price_reg_gross-price_prom_gross);
	discount_pct=divide(discount_rub,price_reg_gross);
  end;
  else do;
	price_fact_gross=price_reg_gross;
	price_fact_net=price_reg_net ;
    discount_rub=0;
	discount_pct=0;
  end;
  promo_ct=coalesce(promo_ct,0);
run;

/* proc casutil; */
/* droptable casdata="PRICE_feat" incaslib="mn_dict" quiet; */
/* promote casdata="price_feat" incaslib="casuser" casout="price_feat" outcaslib="mn_dict"; */
/* run; */

proc fedsql sessref=casauto; 
			create table max_casl.FINAL_FCST_3{options replace=true} as
			select 
				  t1.*
				, t1.FINAL_FCST_UNITS * t2.price_fact_net as FCST_SALE_2
			from MAX_CASL.FINAL_FCST_2 as t1
			left join casuser.price_feat as t2 
			on t1.product_id = t2.product_id
			and t1.pbo_location_id = t2.pbo_location_id 
			and t1.sales_dt = t2.period_dt
	
				
		;
	quit;


proc casutil;
	promote 
		casdata="FINAL_FCST_3" 
		incaslib="max_casl" 
		casout="FINAL_FCST_3" 
		outcaslib="max_casl"
	;
run;
	