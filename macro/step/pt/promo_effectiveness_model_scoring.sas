/* 
	Сборка скоринговых витрин для моделей n_a и t_a.

	1. Сборка скоринговой витрины.
	2. Прогнозирование обученной моделью.  

*/

/* Создаем список уникальных названий мехник промо акций */
data public.unique_promo_mechanics_name;
input promo_mechanics_name $40.;
datalines;
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
;

/*** 1. Сборка скоринговой витрины ***/
%macro scoring_building(
	promo_lib = casuser, 
	ia_promo = future_promo,
	ia_promo_x_pbo = promo_pbo_enh,
	ia_promo_x_product = promo_prod_enh,
	calendar_start = '01jan2017'd,
	calendar_end = '01jan2022'd
	);

	/*
		Макрос, который собирает обучающую выборку для модели прогнозирующей
			na (и ta).
		Схема вычислений:
		1. Вычисление каркаса таблицы промо акций: промо, ПБО, товара, интервал, механика
		2. One hot кодировка механики промо акции
		3. Количество товаров, участвующих в промо (количество уникальных product_id),
			количество позиций (количество уникальных option_number), 
			количество единиц товара, необходимое для покупки
		4. TRP <--- TODO
		5. Пускай у нас имеется k товарных категорий, тогда создадим вектор размерности k.
			Каждая компонента этого вектора описывает количество товаров данной 
			категории участвующих в промо.
		6. Атрибуты ПБО
		7. Календарные признаки и праздники
		8. Признаки описывающие трафик ресторана (количество чеков)
		9. Признаки описывающие продажи промо товаров
		10. Добавление целевой переменной
	
		Параметры:
		----------
			* promo_lib: библиотека, где лежат таблицы с промо (предполагается,
				что таблицы лежат в cas)
			* ia_promo: название таблицы с информацией о промо 
			* ia_promo_x_pbo: название таблицы с привязкой промо к ресторнам
			* ia_promo_x_product: название таблицы с привязкой промо к товарам
			* calendar_start : старт интервала формирования календарных признаков
			* calendar_end : конец интервала формирования календарных признаков
		Выход:
		------
			* Запромоученая в public и скопированная в nac таблица na_train
	*/	

	/*** 1. Вычисление каркаса таблицы промо акций ***/
	data public.pbo_lvl_all;
		set nac.pbo_lvl_all;
	run;

	data public.product_lvl_all;
		set nac.product_lvl_all;
	run;
	
	/* Добавляем к таблице промо ПБО и товары */
	proc fedsql sessref = casauto;
		create table public.ia_promo_x_pbo_leaf{options replace = true} as 
			select distinct
				t1.promo_id,
				t2.PBO_LEAF_ID as pbo_location_id
			from
				&promo_lib..&ia_promo_x_pbo. as t1,
				public.pbo_lvl_all as t2
			where
				t1.pbo_location_id = t2.PBO_LOCATION_ID
		;
		create table public.ia_promo_x_product_leaf{options replace = true} as 
			select distinct
				t1.promo_id,
				t1.OPTION_NUMBER,
				t1.PRODUCT_QTY,
				t2.product_LEAF_ID as product_id
			from
				&promo_lib..&ia_promo_x_product. as t1,
				public.product_lvl_all as t2
			where
				t1.product_id = t2.product_id
		;
	quit;

	/* Формируем каркас витрины */
	proc fedsql sessref=casauto;
		create table public.promo_skelet{options replace = true} as 
			select
				t1.PROMO_ID,
				t2.pbo_location_id,
				t1.START_DT,
				t1.END_DT,
				(t1.END_DT - t1.START_DT) as promo_lifetime,
				t1.CHANNEL_CD,
				t1.PROMO_GROUP_ID,
				t1.NP_GIFT_PRICE_AMT,
				compress(promo_mechanics,'', 'ak') as promo_mechanics_name,
				1 as promo_flag		
			from
				&promo_lib..&ia_promo. as t1
			left join
				public.ia_promo_x_pbo_leaf as t2
			on 
				t1.PROMO_ID = t2.PROMO_ID
		;
	quit;
	
	/* Расшиваем интервалы по дням */
	data public.na_abt0;
		set public.promo_skelet;
		format sales_dt DATE9.;
		do sales_dt=start_dt to end_dt;
			output;
		end;
	run;

	/* Оставляем только текущий год */
	proc fedsql sessref=casauto;
		create table public.na_abt1{options replace=true} as
			select
				*
			from
				public.na_abt0
			where
				year(sales_dt) = year(&ETL_CURRENT_DT_DB)
		;
	quit;
	
	proc casutil;
		droptable casdata="pbo_lvl_all" incaslib="public" quiet;
		droptable casdata="product_lvl_all" incaslib="public" quiet;
		droptable casdata="promo_skelet" incaslib="public" quiet;
	run;
	

	/*** 2. One hot кодировка механики промо акции ***/
	
	/* Определяем механики промо акций */
	proc fedsql sessref=casauto;
		create table public.promo_mechanics{options replace=true} as
			select distinct
				promo_id,
				promo_mechanics_name,
				promo_flag
			from
				public.na_abt1
		;
	quit;

	/* Джоиним одно с другим */
	proc fedsql sessref=casauto;
		create table public.all_combination{options replace=true} as
			select
				t1.promo_id,
				trim(t2.promo_mechanics_name) as promo_mechanics_name
			from
				(select distinct promo_id from public.promo_mechanics) as t1,
				public.unique_promo_mechanics_name as t2
		;
	quit;
		
	/* Заполняем пропуски нулями */
	proc fedsql sessref=casauto;
		create table public.promo_mechanics_zero{options replace=true} as
			select
				t1.promo_id,
				t1.promo_mechanics_name,
				coalesce(t2.promo_flag, 0) as promo_flag
			from
				public.all_combination as t1
			left join
				public.promo_mechanics as t2
			on
				t1.promo_id = t2.promo_id and
				t1.promo_mechanics_name = t2.promo_mechanics_name
		;
	quit;
	
	/* Транспонируем механику промо в вектор */
	proc cas;
	transpose.transpose /
		table = {
			name="promo_mechanics_zero",
			caslib="public",
			groupby={"promo_id"}}
		transpose={"promo_flag"} 
		id={"promo_mechanics_name"} 
		casout={name="promo_mechanics_one_hot", caslib="public", replace=true};
	quit;
	
	/* Добавляем переменные к витрине */
	proc fedsql sessref=casauto;
		create table public.na_abt2{options replace=true} as
			select
				t1.pbo_location_id,
				t1.promo_lifetime,
				t1.CHANNEL_CD,
				t1.NP_GIFT_PRICE_AMT,
				t1.PROMO_GROUP_ID,
				t1.sales_dt,
				t2.*
			from
				public.na_abt1 as t1
			left join
				public.promo_mechanics_one_hot as t2
			on
				t1.promo_id = t2.promo_id
		;
	quit;
	
	proc casutil;
		droptable casdata="na_abt0" incaslib="public" quiet;
		droptable casdata="na_abt1" incaslib="public" quiet;
		droptable casdata="promo_mechanics" incaslib="public" quiet;
		droptable casdata="promo_mechanics_one_hot" incaslib="public" quiet;
		droptable casdata="promo_mechanics_zero" incaslib="public" quiet;
		droptable casdata="unique_promo_mechanics_name" incaslib="public" quiet;
		droptable casdata="all_combination" incaslib="public" quiet;
	run;
	

	/****** 
		3. Количество товаров, участвующих в промо (количество уникальных
			product_id), количество позиций (количество уникальных option_number), 
			количество единиц товара, необходимое для покупки 
	******/
	proc fedsql sessref=casauto;
		/* Количество товаров, позиций участвующих в промо */
		create table public.product_characteristics{options replace=true} as
			select
				promo_id,
				max(option_number) as number_of_options,
				count(distinct product_id) as number_of_products
			from
				public.ia_promo_x_product_leaf
			group by
				promo_id
		;
		/* Количество единиц товара, необходимое для покупки */
		create table public.product_characteristics2{options replace=true} as
			select
				t1.promo_id,
				sum(product_qty) as necessary_amount
			from (
				select distinct
					promo_id,
					option_number,
					PRODUCT_QTY
				from
					public.ia_promo_x_product_leaf
			) as t1
			group by
				t1.promo_id
		;
	quit;
	
	/* Добавляем признаки в витрину */
	proc fedsql sessref=casauto;
		create table public.na_abt3{options replace=true} as
			select
				t1.*,
				t2.number_of_options,
				t2.number_of_products,
				t3.necessary_amount
			from
				public.na_abt2 as t1
			left join
				public.product_characteristics as t2
			on
				t1.promo_id = t2.promo_id
			left join
				public.product_characteristics2 as t3
			on
				t1.promo_id = t3.promo_id	
		;
	quit;
	
	proc casutil;
		droptable casdata="na_abt2" incaslib="public" quiet;
		droptable casdata="product_characteristics" incaslib="public" quiet;
		droptable casdata="product_characteristics2" incaslib="public" quiet;
	run;


	/****** 4. Добавление TRP: TODO ******/

	/****** 
		5. Пускай у нас имеется k товарных категорий,
		 тогда создадим вектор размерности k. Каждая компонента этого
		 вектора описывает количество товаров данной категории участвующих в промо. 
	******/
	
	/* Копируем из nac в  public справочник товаров */
	data public.product_dictionary_ml(replace=yes drop=prod_lvl2_name);
		set nac.product_dictionary_ml;
	run;

	proc fedsql sessref=casauto;
		create table public.product_dictionary_ml{options replace=true} as
			select
				t1.*,
				compress(t1.prod_lvl2_nm,'', 'ak') as prod_lvl2_name
			from
				public.product_dictionary_ml as t1
		;
	quit;

	/* Считаем количество товаров в категории */
	proc fedsql sessref=casauto;
		create table public.promo_category{options replace=true} as
			select
				t1.promo_id,
				t2.prod_lvl2_name,
				count(distinct t1.product_id) as count_promo
			from
				public.ia_promo_x_product_leaf as t1
			inner join
				public.product_dictionary_ml as t2
			on
				t1.product_id = t2.product_id
			group by
				t1.promo_id,
				t2.prod_lvl2_name
		;
	quit;
	
	/* Транспонируем таблицу */
	proc cas;
	transpose.transpose /
	   table={name="promo_category", caslib="public", groupby={"promo_id"}} 
	   transpose={"count_promo"} 
	   id={"prod_lvl2_name"} 
	   casout={name="promo_category_transposed", caslib="public", replace=true};
	quit;
	
	/* Заменяем пропуски на нули */
	data public.promo_category_transposed_zero;
		set public.promo_category_transposed;
		drop _name_;
		array change _numeric_;
	    	do over change;
	            if change=. then change=0;
	        end;
	run;
	
	/* Добавляем признаки в витрину */
	proc fedsql sessref=casauto;
		create table public.na_abt4{options replace=true} as
			select
				t1.*,
				t2.Breakfast,
				t2.ColdDrinks,
				t2.Condiments,
				t2.Desserts,
				t2.Fries,
				t2.HotDrinks,
				t2.McCafe,
				t2.Nonproduct,
				t2.Nuggets,
				t2.SNCORE,
				t2.SNEDAP,
				t2.SNPREMIUM,
				t2.Shakes,
				t2.StartersSalad,
				t2.UndefinedProductGroup,
				t2.ValueMeal
			from
				public.na_abt3 as t1
			left join
				public.promo_category_transposed_zero as t2
			on
				t1.promo_id = t2.promo_id
		;
	quit;
	
	proc casutil;
		droptable casdata="promo_category" incaslib="public" quiet;
		droptable casdata="promo_category_transposed" incaslib="public" quiet;
		droptable casdata="promo_category_transposed_zero" incaslib="public" quiet;
		droptable casdata="na_abt3" incaslib="public" quiet;
	run;

	/****** 6. Атрибуты ПБО ******/
	data public.pbo_dictionary_ml;
		set nac.pbo_dictionary_ml;
	run;
	
	proc fedsql sessref=casauto;
		create table public.na_abt5{options replace=true} as
			select
				t1.*,
				t2.lvl3_id,
				t2.lvl2_id,
				t2.A_AGREEMENT_TYPE_id as agreement_type_id,
				t2.A_BREAKFAST_id as breakfast_id,
				t2.A_BUILDING_TYPE_id as building_type_id,
				t2.A_COMPANY_id as company_id,
				t2.A_DELIVERY_id as delivery_id,
				t2.A_DRIVE_THRU_id as drive_thru_id,
				t2.A_MCCAFE_TYPE_id as mccafe_type_id,
				t2.A_PRICE_LEVEL_id as price_level_id,
				t2.A_WINDOW_TYPE_id as window_type_id
			from
				public.na_abt4 as t1
			left join
				public.pbo_dictionary_ml as t2
			on
				t1.pbo_location_id = t2.pbo_location_id
		;
	quit;
	
	proc casutil;
		droptable casdata="na_abt4" incaslib="public" quiet;
	run;


	/****** 7. Календарные признаки и праздники ******/
	data work.cldr_prep;
		retain date &calendar_start.;
		do while(date <= &calendar_end.);
			output;
			date + 1;		
		end;
		format date ddmmyy10.;
	run;
	
	proc sql;
		create table work.cldr_prep_features as 
			select
				date, 
				week(date) as week,
				weekday(date) as weekday,
				month(date) as month,
				year(date) as year,
				(case
					when weekday(date) in (1, 7) then 1
					else 0
				end) as weekend_flag
			from
				work.cldr_prep
		;
	quit;
	
	/* загружаем в cas */
	data public.russia_weekend;
	set nac.russia_weekend;
	weekend_flag=1;
	run;
	
	/* транспонируем russia_weekend */
	proc cas;
	transpose.transpose /
	   table={name="russia_weekend", caslib="public", groupby={"date"}} 
	   transpose={"weekend_flag"} 
	   id={"weekend_name"} 
	   casout={name="russia_weekend_transposed", caslib="public", replace=true};
	quit;
	
	/* Заменяем пропуски на нули */
	data public.russia_weekend_transposed_zero;
		set public.russia_weekend_transposed;
		drop _name_;
		array change _numeric_;
	    	do over change;
	            if change=. then change=0;
	        end;
	run;
	
	/* Объединяем государственные выходные с субботой и воскресеньем */
	proc sql;
		create table work.cldr_prep_features2 as 
			select
				t1.date,
				t1.week,
				t1.weekday,
				t1.month,
				t1.year,
				t1.weekend_flag as regular_weekend_flag,
				case
					when t2.date is not missing then 1
					else t1.weekend_flag
				end as weekend_flag
			from
				work.cldr_prep_features as t1
			left join
				nac.russia_weekend as t2
			on
				t1.date = t2.date
		;
	quit;
	
	/* Загружаем в cas */
	data public.cldr_prep_features2;
		set work.cldr_prep_features2;
	run;
	
	/* Добавляем к витрине */
	proc fedsql sessref = casauto;
		create table public.na_abt6{options replace = true} as
			select
				t1.*,
				t2.week,
				t2.weekday,
				t2.month,
				t2.year,
				t2.regular_weekend_flag,
				t2.weekend_flag,
				coalesce(t3.Christmas, 0) as Christmas,
				coalesce(t3.Christmas_Day, 0) as Christmas_Day,
				coalesce(t3.Day_After_New_Year, 0) as Day_After_New_Year,
				coalesce(t3.Day_of_Unity, 0) as Day_of_Unity,
				coalesce(t3.Defendence_of_the_Fatherland, 0) as Defendence_of_the_Fatherland,
				coalesce(t3.International_Womens_Day, 0) as International_Womens_Day,
				coalesce(t3.Labour_Day, 0) as Labour_Day,
				coalesce(t3.National_Day, 0) as National_Day,
				coalesce(t3.New_Year_shift, 0) as New_Year_shift, 
 				coalesce(t3.New_year, 0) as New_year,
				coalesce(t3.Victory_Day, 0) as Victory_Day		 
			from
				public.na_abt5 as t1
			left join
				public.cldr_prep_features2 as t2
			on
				t1.sales_dt = t2.date
			left join
				public.russia_weekend_transposed as t3
			on
				t1.sales_dt = t3.date
		;
	quit;
	
	proc casutil;
		droptable casdata="na_abt5" incaslib="public" quiet;
		droptable casdata="russia_weekend_transposed" incaslib="public" quiet;
		droptable casdata="russia_weekend_transposed_zero" incaslib="public" quiet;
		droptable casdata="cldr_prep_features2" incaslib="public" quiet;
		droptable casdata="russia_weekend" incaslib="public" quiet;
	run;

	
	/****** 8. Признаки описывающие трафик ресторана ******/

	/* Сохраняем таблицы для сборки скоринговой выборки */
	data public.gc_aggr_smart;
		set nac.gc_aggr_smart;
	run;

	data public.gc_aggr_dump;
		set nac.gc_aggr_dump;
	run;

	/* Добавляем к витрине характеристики трафика ресторана */
	proc fedsql sessref=casauto;
		create table public.na_abt7{options replace=true} as
			select
				t1.*,
				coalesce(t2.mean_receipt_qty, t3.mean_receipt_qty) as mean_receipt_qty,
				coalesce(t2.std_receipt_qty, t3.std_receipt_qty) as std_receipt_qty	
			from
				public.na_abt6 as t1
			left join
				public.gc_aggr_smart as t2
			on
				(t1.year - 1) = t2.year and
				t1.pbo_location_id = t2.pbo_location_id and
				t1.month = t2.month and
				t1.weekday = t2.weekday
			left join
				public.gc_aggr_dump as t3
			on
				(t1.year - 1) = t3.year and
				t1.month = t3.month and
				t1.weekday = t3.weekday
		;
	quit;
	
	proc casutil;
		droptable casdata="gc_aggr_smart" incaslib="public" quiet;
		droptable casdata="gc_aggr_dump" incaslib="public" quiet;
		droptable casdata="na_abt6" incaslib="public" quiet;
	run;


	/****** 9. Признаки описывающие продажи промо товаров ******/

	/* Выгружаем таблицу в cas */
	data public.pmix_mastercode_sum;
		set nac.pmix_mastercode_sum;
	run;
	
	/* Снова создадим таблицу с промо акциями */
	proc fedsql sessref=casauto;
		create table public.promo_ml{options replace = true} as 
			select
				t1.PROMO_ID,
				t1.start_dt,
				t1.end_dt,
				t1.promo_mechanics,
				t3.product_id,
				t2.pbo_location_id
			from
				&promo_lib..&ia_promo. as t1 
			left join
				public.ia_promo_x_pbo_leaf as t2
			on 
				t1.PROMO_ID = t2.PROMO_ID
			left join
				public.ia_promo_x_product_leaf as t3
			on
				t1.PROMO_ID = t3.PROMO_ID
		;
	quit;
	
	/* Меняем товары на мастеркоды  */
	proc fedsql sessref=casauto;
		create table public.promo_ml2{options replace = true} as 
			select distinct
				t1.PROMO_ID,
				t2.PROD_LVL4_ID,
				t1.pbo_location_id
			from
				public.promo_ml as t1
			inner join
				public.product_dictionary_ml as t2
			on
				t1.product_id = t2.product_id
		;
	quit;
	
	/* Соединяем продажи с промо */
	proc fedsql sessref=casauto;
		create table public.promo_ml3{options replace = true} as 
			select
				t1.promo_id,
				t1.pbo_location_id,
				t2.sales_dt,
				mean(t2.sales_qty) as mean_sales_qty
			from
				public.promo_ml2 as t1
			left join
				public.pmix_mastercode_sum as t2
			on
				t1.PROD_LVL4_ID = t2.PROD_LVL4_ID and
				t1.pbo_location_id = t2.pbo_location_id
			group by
				t1.promo_id,
				t1.pbo_location_id,
				t2.sales_dt			
		;
	quit;
	
	/* Считаем агрегаты Промо, ПБО, год, месяц, день недели */
	proc fedsql sessref=casauto;
		create table public.pmix_aggr_smart{options replace=true} as
			select
				t1.promo_id,
				t1.pbo_location_id,
				t1.year,
				t1.month,
				t1.weekday,
				mean(t1.mean_sales_qty) as mean_sales_qty,
				std(t1.mean_sales_qty) as std_sales_qty
			from (
				select
					t1.promo_id,
					t1.pbo_location_id,
					year(t1.sales_dt) as year,
					month(t1.sales_dt) as month,
					weekday(t1.sales_dt) as weekday,
					t1.mean_sales_qty
				from
					public.promo_ml3 as t1
			) as t1
			group by
				t1.promo_id,
				t1.pbo_location_id,
				t1.year,
				t1.month,
				t1.weekday
		;
	quit;
	
	/* Считаем агрегаты Промо, год, месяц, день недели */
	proc fedsql sessref=casauto;
		create table public.pmix_aggr_dump{options replace=true} as
			select
				t1.promo_id,
				t1.year,
				t1.month,
				t1.weekday,
				mean(t1.mean_sales_qty) as mean_sales_qty,
				std(t1.mean_sales_qty) as std_sales_qty
			from (
				select
					t1.promo_id,
					year(t1.sales_dt) as year,
					month(t1.sales_dt) as month,
					weekday(t1.sales_dt) as weekday,
					t1.mean_sales_qty
				from
					public.promo_ml3 as t1
			) as t1
			group by
				t1.promo_id,
				t1.year,
				t1.month,
				t1.weekday
		;
	quit;

	proc casutil;
		droptable casdata="promo_effectivness_scoring" incaslib="public" quiet;
	run;
	
	/* Добавляем к витрине характеристики трафика ресторана */
	proc fedsql sessref=casauto;
		create table public.promo_effectivness_scoring{options replace=true} as
			select
				t1.PBO_LOCATION_ID,
				t1.PROMO_LIFETIME,
				put(t1.channel_cd, $12.) as channel_cd,
				t1.np_gift_price_amt,
				t1.promo_group_id,
				t1.sales_dt,
				t1.promo_id,
				t1.Bundle,
				t1.Discount,
				t1.EVMSet,
				t1.Giftforpurchaseforproduct,
				t1.GiftforpurchaseNonProduct,
				t1.GiftforpurchaseSampling,
				t1.NPPromoSupport,
				t1.OtherDiscountforvolume,
				t1.Pairs,
				t1.Pairsdifferentcategories,
				t1.Productlineextension,
				t1.ProductnewlaunchLTO,
				t1.ProductnewlaunchPermanentinclite,
				t1.Productrehitsameproductnolineext,
				t1.Temppricereductiondiscount,
				t1.Undefined,
				t1.NUMBER_OF_OPTIONS,
				t1.NUMBER_OF_PRODUCTS,
				t1.NECESSARY_AMOUNT,
				t1.Breakfast,
				t1.ColdDrinks,
				t1.Condiments,
				t1.Desserts,
				t1.Fries,
				t1.HotDrinks,
				t1.McCafe,
				t1.Nonproduct,
				t1.Nuggets,
				t1.SNCORE,
				t1.SNEDAP,
				t1.SNPREMIUM,
				t1.Shakes,
				t1.StartersSalad,
				t1.UndefinedProductGroup,
				t1.ValueMeal,
				t1.LVL3_ID,
				t1.LVL2_ID,
				t1.AGREEMENT_TYPE_ID,
				t1.BREAKFAST_ID,
				t1.BUILDING_TYPE_ID,
				t1.COMPANY_ID,
				t1.DELIVERY_ID,
				t1.DRIVE_THRU_ID,
				t1.MCCAFE_TYPE_ID,
				t1.PRICE_LEVEL_ID,
				t1.WINDOW_TYPE_ID,
				t1.week,
				t1.weekday,
				t1.month,
				t1.year,
				t1.regular_weekend_flag,
				t1.weekend_flag,
				t1.CHRISTMAS,
				t1.CHRISTMAS_DAY,
				t1.DAY_AFTER_NEW_YEAR,
				t1.DAY_OF_UNITY,
				t1.DEFENDENCE_OF_THE_FATHERLAND,
				t1.INTERNATIONAL_WOMENS_DAY,
				t1.LABOUR_DAY,
				t1.NATIONAL_DAY,
				t1.NEW_YEAR_SHIFT,
				t1.NEW_YEAR,
				t1.VICTORY_DAY,
				t1.MEAN_RECEIPT_QTY,
				t1.STD_RECEIPT_QTY,
				coalesce(t2.mean_sales_qty, t3.mean_sales_qty) as MEAN_SALES_QTY,
				coalesce(t2.std_sales_qty, t3.std_sales_qty) as STD_SALES_QTY,
				. as n_a,
				. as t_a
			from
				public.na_abt7 as t1
			left join
				public.pmix_aggr_smart as t2
			on
				t1.promo_id = t2.promo_id and
				(t1.year - 1) = t2.year and
				t1.pbo_location_id = t2.pbo_location_id and
				t1.month = t2.month and
				t1.weekday = t2.weekday
			left join
				public.pmix_aggr_dump as t3
			on
				t1.promo_id = t3.promo_id and
				(t1.year - 1) = t3.year and
				t1.month = t3.month and
				t1.weekday = t3.weekday
		;
	quit;
	
	proc casutil;
		droptable casdata="pmix_mastercode_sum" incaslib="public" quiet;
		droptable casdata="na_abt7" incaslib="public" quiet;
		droptable casdata="promo_ml2" incaslib="public" quiet;
		droptable casdata="promo_ml3" incaslib="public" quiet;
		droptable casdata="pmix_aggr_smart" incaslib="public" quiet;
		droptable casdata="pmix_aggr_dump" incaslib="public" quiet;

	    promote casdata="promo_effectivness_scoring" incaslib="public" outcaslib="public";
	run;
	
	/* Дополнительно сохраняем витрину в Nac */
	data nac.promo_effectivness_scoring;
		set public.promo_effectivness_scoring;
	run;

%mend;


%macro promo_effectivness_predict(
	target = na,
	data = public.promo_effectivness_scoring
	); 
	/*
		Макрос, который прогнозирует эффективность промо акций при
		помощи обученных моеделей.
		Параметры:
		----------
			* target : целевая переменная (na либо ta)
			* data : скоринговая выборка
	*/
	/****** Скоринг ******/
    proc astore;
        upload RSTORE=public.&target._prediction_model store="/data/ETL_BKP/&target._prediction_model";
    run;


	proc casutil;
	    droptable casdata="promo_effectivness_&target._predict" incaslib="public" quiet;
	run;


	proc astore;
		score data=&data.
		copyvars=(_all_)
		rstore=public.&target._prediction_model
		out=public.promo_effectivness_&target._predict;
	quit;
	
	proc casutil;
	    promote casdata="promo_effectivness_&target._predict" incaslib="public" outcaslib="public";
	run;

	/* Сохраняем прогноз в nac */
	data nac.promo_effectivness_&target._predict;
		set public.promo_effectivness_&target._predict;
	run;

%mend;

