proc fedsql sessref=casauto;
	select
		divide(nmiss(mean_sales_qty), count(1)) as _miss_pcnt,
		divide(nmiss(std_sales_qty), count(1)) as std_sales_qty_miss_pcnt
	from
		public.na_abt14
	;
quit;

proc fedsql sessref=casauto;
	create table public.bads{options replace=true} as
		select
			*
		from
			public.na_abt9
		where
			mean_sales_qty is missing
	;
quit;

data bads;
	set public.bads(obs=100);
run;

%let promo_id = 785;
%let pbo_location_id = 70316;

data public.product;
	set etl_ia.product(
		where=(
			&ETL_CURRENT_DTTM. <= valid_to_dttm and
			&ETL_CURRENT_DTTM. >= valid_from_dttm
		)
	);
run;

proc fedsql sessref=casauto;
	select
		*
	from
		casuser.past_promo
	where
		promo_id = &promo_id.
	;


	select
		t2.product_nm,
		t1.*
	from
		casuser.promo_prod_enh as t1
	inner join 
		public.product as t2
	on
		t1.product_id = t2.product_id
	where
		promo_id = &promo_id.
	;

	select
		t1.*,
		t2.product_nm
	from
		public.promo_ml2 as t1
	inner join
		public.product as t2
	on
		t1.prod_lvl4_id = t2.product_id
	where
		promo_id = &promo_id. and
		pbo_location_id = &pbo_location_id.
	;
	select
		*
	from
		public.pmix_aggr_smart
	where
		promo_id = &promo_id. and
		pbo_location_id = &pbo_location_id.
	order by
		year,
		month,
		weekday
	;

	select
		*
	from
		public.pmix_aggr_dump
	where
		promo_id = &promo_id.
	order by
		year,
		month,
		weekday
	;
quit;

data one_ts;
	set public.promo_ml3(
		where=(
			promo_id = &promo_id. and
			pbo_location_id = &pbo_location_id.
		)
	);
run;

proc sort data=one_ts;
	by sales_dt;
run;

proc sgplot data=one_ts;
	series x=sales_dt y=mean_sales_qty;
run;

/* А собстна когда была первая дата продаж любого товара из этого промо? */
data promo_product;
	set casuser.promo_prod_enh(where=(promo_id=&promo_id.));
run;

proc sql;
	select 
		t1.product_id,
		min(sales_dt) as min_date format date9.
	from
		etl_ia.pmix_sales as t1
	inner join
		promo_product as t2
	on
		t1.product_id = t2.product_id
	where
		&ETL_CURRENT_DTTM. <= valid_to_dttm and
		&ETL_CURRENT_DTTM. >= valid_from_dttm and
		channel_cd = 'ALL'
	group by
		t1.product_id
	;
quit;

proc sql;
	select distinct
		sales_dt
	from
		etl_ia.pmix_sales as t1
	inner join
		promo_product as t2
	on
		t1.product_id = t2.product_id
	where
		&ETL_CURRENT_DTTM. <= valid_to_dttm and
		&ETL_CURRENT_DTTM. >= valid_from_dttm and
		channel_cd = 'ALL'
	order by
		sales_dt
	;
quit;

/* Так, а мастеркоды с каких дат начались? */
proc sql;
	select
		t1.*
	from
		nac.pmix_mastercode_sum as t1
	inner join (
		select
			t1.*,
			t2.product_nm
		from
			public.promo_ml2 as t1
		inner join
			public.product as t2
		on
			t1.prod_lvl4_id = t2.product_id
		where
			promo_id = &promo_id. and
			pbo_location_id = &pbo_location_id.
	) as t2
	on
		t1.prod_lvl4_id = t2.prod_lvl4_id and
		t1.pbo_location_id = t2.pbo_location_id
	order by
		prod_lvl4_id,
		sales_dt
	;
quit;


proc fedsql sessref=casauto;
	select
		min(sales_dt) as min_date
	from
		public.pmix_mastercode_sum as t1
	;
quit;

proc sql;
	create table temp as
		select
			t2.PROD_LVL4_ID,
			t1.product_id,
			t1.sales_dt,
			t1.pbo_location_id,
			sum(t1.sales_qty, t1.sales_qty_promo) as sales_qty	
		from (
			select 
				* 
			from
				etl_ia.pmix_sales
			where
				&ETL_CURRENT_DTTM. <= valid_to_dttm and
				&ETL_CURRENT_DTTM. >= valid_from_dttm and
				channel_cd = 'ALL' and
				pbo_location_id = &pbo_location_id.
		) as t1
		inner join
			nac.product_dictionary_ml as t2
		on
			t1.product_id = t2.product_id
	;
quit;

proc sql;
	select nmiss(prod_lvl4_id) from temp;
quit;

data promo_product;
	set casuser.promo_prod_enh(where=(promo_id=&promo_id.));
run;

proc sql;
	create table promo_temp as
		select
			t1.*
		from
			temp as t1
		inner join
			promo_product as t2
		on
			t1.product_id = t2.product_id
		order by
			prod_lvl4_id,
			product_id,
			sales_dt
	;
quit;

/* Может быть, просто ресторан новый? */
proc sql;
	select
		min(sales_dt) format date9.
	from
		etl_ia.pbo_sales
	where
		pbo_location_id = &pbo_location_id. and
		&ETL_CURRENT_DTTM. <= valid_to_dttm and
		&ETL_CURRENT_DTTM. >= valid_from_dttm

	;
quit;


proc fedsql sessref=casauto;
	select
		*
	from
		public.na_abt14
	where
		promo_id = &promo_id. and
		pbo_location_id = &pbo_location_id.
	order by
		sales_dt		
	;
quit;


/*
	Просто тестирование результатов.
	В дальнейшем этот скрипт можно будет удалить.
*/
%macro daily_double(data);

	proc fedsql sessref=casauto;
		select
			count(1) as cnt
		from (
			select
				channel_cd,
				pbo_location_id,
				product_id,
				sales_dt,
				count(1) as cnt
			from
				&data.
			group by
				channel_cd,
				pbo_location_id,
				product_id,
				sales_dt
		) as t1
		where
			t1.cnt > 1
		;
	quit;

%mend;

%macro weekly_double(data);

	proc fedsql sessref=casauto;
		select
			count(1) as cnt
		from (
			select
				channel_cd,
				pbo_location_id,
				product_id,
				week,
				count(1) as cnt
			from
				&data.
			group by
				channel_cd,
				pbo_location_id,
				product_id,
				week
		) as t1
		where
			t1.cnt > 1
		;
	quit;

%mend;


proc sql;
	select
		nmiss(np_gift_price_amt) as cnt,
		count(1) as all_rows
	from
		nac.na_train
	;
quit;

proc sql;
	select
		count(1) as number_of_rows
	from
		nac.na_train
	where
		PROMO_LIFETIME < 0
	;
quit;


proc surveyselect data=nac.na_train method=srs n=10000
                  out=work.na_train_sample;
run;

proc sgplot data=work.na_train_sample;
	density np_gift_price_amt / type=kernel;
	
run;

%macro count_unique(var);
	proc sql;
		select
			&var.,
			count(1) as cnt
		from
			nac.na_train
		group by
			&var.
		;
	quit;
%mend;


%count_unique(Undefined)