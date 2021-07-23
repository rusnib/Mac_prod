/* TODO. Выгрузить витрину с прогнозом в Python.
	Посмотреть, что не так, где ошибаемся
 */


%let test_threshold = '30nov2020'd;
/* Будущие промо */
proc sql;
	create table work.past_promo as
		select distinct
			promo_id
		from
			nac.na_abt14
		where
			sales_dt <= &test_threshold.
	;
quit;

/* Тест */
proc sql;
	create table work.test as
		select
			t1.*
		from
			nac.na_abt14 as t1
		left join
			work.past_promo as t2
		on
			t1.promo_id = t2.promo_id
		where
			t2.promo_id is missing
	;
quit;

/* Трейн */
proc sql;
	create table work.train as
		select
			t1.*
		from
			nac.na_abt14 as t1
		inner join
			work.past_promo as t2
		on
			t1.promo_id = t2.promo_id

	;
quit;

/* Check rows count */
proc sql;
	select count(1) as cnt from work.train;
	select count(1) as cnt from work.test;
quit;


/* Гиперпараметры моделей */
%let default_hyper_params = seed=12345 loh=0 binmethod=QUANTILE 
	 maxbranch=2 
     assignmissing=useinsearch 
	 minuseinsearch=5
     ntrees=50
     maxdepth=20
     inbagfraction=0.7
     minleafsize=5
     numbin=100
     printtarget
;


data casuser.train;
	set work.train;
run;

%let data = casuser.train;
%let target = n_a;
%let output = test_quality;


/* Стираем результирующие таблицы с обученными моделями */
proc casutil;
	droptable casdata="&output." incaslib="public" quiet;
run;
	
/* Обучение модели */
proc forest data=&data.
	&default_hyper_params.;
	input 
		Bundle
		Discount
		EVMSet
		Giftforpurchaseforproduct
		GiftforpurchaseNonProduct
		GiftforpurchaseSampling
		NPPromoSupport
		OtherDiscountforvolume
		Pairs
		Pairsdifferentcategories
		Productlineextension
		ProductnewlaunchLTO
		ProductnewlaunchPermanentinclite
		Productrehitsameproductnolineext
		Temppricereductiondiscount
		Undefined
		NUMBER_OF_OPTIONS
		NUMBER_OF_PRODUCTS
		NECESSARY_AMOUNT
		Breakfast
		ColdDrinks
		Condiments
		Desserts
		Fries
		HotDrinks
		McCafe
		Nonproduct
		Nuggets
		SNCORE
		SNEDAP
		SNPREMIUM
		Shakes
		StartersSalad
		UndefinedProductGroup
		ValueMeal
		week
		weekday
		month
		year
		regular_weekend_flag
		weekend_flag
		Christmas
		Christmas_Day
		Day_After_New_Year
		Day_of_Unity
		Defendence_of_the_Fatherland
		International_Womens_Day
		Labour_Day
		National_Day
		New_Year_shift
		New_year
		Victory_Day
		MEAN_RECEIPT_QTY
		STD_RECEIPT_QTY
		mean_sales_qty
		std_sales_qty
			/ level = interval;
	input 
		AGREEMENT_TYPE_ID
		BREAKFAST_ID
		BUILDING_TYPE_ID
		COMPANY_ID
		DELIVERY_ID
		DRIVE_THRU_ID
		MCCAFE_TYPE_ID
/* 		PRICE_LEVEL_ID */
		WINDOW_TYPE_ID
		 / level = nominal;
	id promo_id pbo_location_id sales_dt;
	target &target. / level = interval;
	savestate rstore=public.&output.;
	;
run;

proc casutil;
    promote casdata="&output." incaslib="public" outcaslib="public";
run;

data casuser.test;
	set work.test;
run;

proc astore;
	score data=casuser.train
	copyvars=(_all_)
	rstore=public.test_quality
	out=casuser.train_prediction
	;
quit;

options casdatalimit=20G;
data nac.test_prediction;
	set casuser.test_prediction;
run; 

data nac.train_prediction;
	set  casuser.train_prediction;
run;



%let bad_promo = 1506, 1504, 1092, 1116, 1526;
%let bad_pbo = 21034,21069, 21097, 21054, 70331, 21062;

/* Check quality metric */
proc fedsql sessref=casauto;
	select
		mean(divide(abs(p_n_a-n_a), n_a)) as mape_without_filter,
		sum(abs(p_n_a-n_a))/sum(n_a) as wape_without_filter
	from
		casuser.test_prediction
	;

	select
		mean(divide(abs(p_n_a-n_a), n_a)) as mape_with_filter,
		sum(abs(p_n_a-n_a))/sum(n_a) as wape_with_filter
	from
		casuser.test_prediction
	where 
		promo_id not in (&bad_promo.) and
		pbo_location_id not in (&bad_pbo.)
	;

	select
		pbo_location_id,
		mean(divide(abs(p_n_a-n_a), n_a)) as mape,
		sum(abs(p_n_a-n_a))/sum(n_a) as wape
	from
		casuser.test_prediction
	where 
		promo_id not in (&bad_promo.) 
	group by
		pbo_location_id
	order by
		mape
	;
	select
		promo_id,
		mean(divide(abs(p_n_a-n_a), n_a)) as mape,
		sum(abs(p_n_a-n_a))/sum(n_a) as wape
	from
		casuser.test_prediction
	group by
		promo_id
	order by
		WAPE
	;
quit;

/* 6 mape without filter, 3 with filter */


