/* Скрипт собирающий обучающую выборку для модели, прогнозирующей Na */

/* 
	Список выходных дней в РФ с 2018 по 2023.
	Внутрь скрипта это data step положить не получается, поэтому
	вынесен это шаг отдельно
*/
data nac.russia_weekend;
input date :yymmdd10. weekend_name $64.;
format date yymmddd10.;
datalines;
2018-01-01 New_year
2018-01-02 Day_After_New_Year
2018-01-07 Christmas
2018-02-23 Defendence_of_the_Fatherland
2018-03-08 International_Womens_Day
2018-05-09 Victory_Day
2018-06-12 National_Day
2018-11-04 Day_of_Unity
2018-05-01 Labour_Day
2018-12-25 Christmas_Day
2019-01-01 New_year
2019-01-02 Day_After_New_Year
2019-01-07 Christmas
2019-02-23 Defendence_of_the_Fatherland
2019-03-08 International_Womens_Day
2019-05-09 Victory_Day
2019-06-12 National_Day
2019-11-04 Day_of_Unity
2019-05-01 Labour_Day
2019-12-25 Christmas_Day
2020-01-01 New_year
2020-01-02 Day_After_New_Year
2020-01-07 Christmas
2020-02-23 Defendence_of_the_Fatherland
2020-03-08 International_Womens_Day
2020-05-09 Victory_Day
2020-06-12 National_Day
2020-11-04 Day_of_Unity
2020-05-01 Labour_Day
2020-12-25 Christmas_Day
2021-01-01 New_year
2021-01-02 Day_After_New_Year
2021-01-07 Christmas
2021-02-23 Defendence_of_the_Fatherland
2021-03-08 International_Womens_Day
2021-05-09 Victory_Day
2021-06-12 National_Day
2021-11-04 Day_of_Unity
2021-05-01 Labour_Day
2021-12-25 Christmas_Day
2022-01-01 New_year
2022-01-02 Day_After_New_Year
2022-01-07 Christmas
2022-02-23 Defendence_of_the_Fatherland
2022-03-08 International_Womens_Day
2022-05-09 Victory_Day
2022-06-12 National_Day
2022-11-04 Day_of_Unity
2022-05-01 Labour_Day
2022-01-03 New_Year_shift
2022-12-25 Christmas_Day
2023-01-01 New_year
2023-01-02 Day_After_New_Year
2023-01-07 Christmas
2023-02-23 Defendence_of_the_Fatherland
2023-03-08 International_Womens_Day
2023-05-09 Victory_Day
2023-06-12 National_Day
2023-11-04 Day_of_Unity
2023-05-01 Labour_Day
2023-01-02 New_Year_shift
2023-12-25 Christmas_Day
;
run;


%macro promo_effectiveness_abt_building(
	promo_lib = public, 
	ia_promo = ia_promo,
	ia_promo_x_pbo = ia_promo_x_pbo,
	ia_promo_x_product = ia_promo_x_product,
	hist_start_dt = date '2019-01-01',
	filter = t1.channel_cd = 'ALL',
	calendar_start = '01jan2017'd,
	calendar_end = '01jan2022'd
);
/*
	Макрос, который собирает обучающую выборку для модели прогнозирующей
		na (и ta).
	Схема вычислений:
	1. Вычисление каркаса таблицы промо акций: промо, ПБО, товар, интервал, механика
	2. One hot кодировка механики промо акции
	3. Количество товаров, участвующих в промо (количество уникальных product_id),
		количество позиций (количество уникальных option_number), 
		количество единиц товара, необходимое для покупки
	4. TRP <--- TODO.
	5. Цены <--- TODO.
	6. Пускай у нас имеется k товарных категорий, тогда создадим вектор размерности k.
		Каждая компонента этого вектора описывает количество товаров данной 
		категории участвующих в промо.
	7. Атрибуты ПБО
	8. Календарные признаки и праздники
	9. Признаки описывающие трафик ресторана (количество чеков)
	10. Признаки описывающие продажи промо товаров
	11. Добавление целевой переменной

	Параметры:
	----------
		* promo_lib: библиотека, где лежат таблицы с промо (предполагается,
			что таблицы лежат в cas)
		* ia_promo: название таблицы с информацией о промо 
		* ia_promo_x_pbo: название таблицы с привязкой промо к ресторнам
		* ia_promo_x_product: название таблицы с привязкой промо к товарам
		* hist_start_dt: рассматриваем все промо, начавшиеся после этой даты
		* filter : фильтр для таблицы с промо (например, убрать каналы)
		* calendar_start : старт интервала формирования календарных признаков
		* calendar_end : конец интервала формирования календарных признаков
	Выход:
	------
		* Запромоученая в public и скопированная в nac таблица na_train
*/	

	/****** 1. Вычисление каркаса таблицы промо акций ******/

	/* Загружаем товарную и географическую иерархии */
	proc casutil;
		load data=etl_ia.pbo_loc_hierarchy(
			where=(
				&ETL_CURRENT_DTTM. <= valid_to_dttm and
				&ETL_CURRENT_DTTM. >= valid_from_dttm
			)
		) casout='ia_pbo_loc_hierarchy' outcaslib='public' replace;	
	
		load data=etl_ia.product_hierarchy(
			where=(
				&ETL_CURRENT_DTTM. <= valid_to_dttm and
				&ETL_CURRENT_DTTM. >= valid_from_dttm
			)
		) casout='ia_product_hierarchy' outcaslib='public' replace;
	run;
	
	/* Создаем таблицу связывающую PBO на листовом уровне и на любом другом */
	proc fedsql sessref=casauto;
		create table public.pbo_hier_flat{options replace=true} as
			select
				t1.pbo_location_id, 
				t2.PBO_LOCATION_ID as LVL3_ID,
				t2.PARENT_PBO_LOCATION_ID as LVL2_ID, 
				1 as LVL1_ID
			from 
				(select * from public.ia_pbo_loc_hierarchy where pbo_location_lvl=4) as t1
			left join 
				(select * from public.ia_pbo_loc_hierarchy where pbo_location_lvl=3) as t2
			on t1.PARENT_PBO_LOCATION_ID=t2.PBO_LOCATION_ID
		;
		create table public.lvl4{options replace=true} as 
			select distinct
				pbo_location_id as pbo_location_id,
				pbo_location_id as pbo_leaf_id
			from
				public.pbo_hier_flat
		;
		create table public.lvl3{options replace=true} as 
			select distinct
				LVL3_ID as pbo_location_id,
				pbo_location_id as pbo_leaf_id
			from
				public.pbo_hier_flat
		;
		create table public.lvl2{options replace=true} as 
			select distinct
				LVL2_ID as pbo_location_id,
				pbo_location_id as pbo_leaf_id
			from
				public.pbo_hier_flat
		;
		create table public.lvl1{options replace=true} as 
			select 
				1 as pbo_location_id,
				pbo_location_id as pbo_leaf_id
			from
				public.pbo_hier_flat
		;
	quit;
	
	/* Соединяем в единый справочник ПБО */
	data public.pbo_lvl_all;
		set public.lvl4 public.lvl3 public.lvl2 public.lvl1;
	run;
	
	/* Создаем таблицу связывающую товары на листовом уровне и на любом другом */
	proc fedsql sessref=casauto;
	   create table public.product_hier_flat{options replace=true} as
			select t1.product_id, 
				   t2.product_id  as LVL4_ID,
				   t3.product_id  as LVL3_ID,
				   t3.PARENT_product_id as LVL2_ID, 
				   1 as LVL1_ID
			from 
			(select * from public.ia_product_hierarchy where product_lvl=5) as t1
			left join 
			(select * from public.ia_product_hierarchy where product_lvl=4) as t2
			on t1.PARENT_PRODUCT_ID=t2.PRODUCT_ID
			left join 
			(select * from public.ia_product_hierarchy where product_lvl=3) as t3
			on t2.PARENT_PRODUCT_ID=t3.PRODUCT_ID
	 	;
		create table public.lvl5{options replace=true} as 
			select distinct
				product_id as product_id,
				product_id as product_leaf_id
			from
				public.product_hier_flat
		;
		create table public.lvl4{options replace=true} as 
			select distinct
				LVL4_ID as product_id,
				product_id as product_leaf_id
			from
				public.product_hier_flat
		;
		create table public.lvl3{options replace=true} as 
			select distinct
				LVL3_ID as product_id,
				product_id as product_leaf_id
			from
				public.product_hier_flat
		;
		create table public.lvl2{options replace=true} as 
			select distinct
				LVL2_ID as product_id,
				product_id as product_leaf_id
			from
				public.product_hier_flat
		;
		create table public.lvl1{options replace=true} as 
			select distinct
				1 as product_id,
				product_id as product_leaf_id
			from
				public.product_hier_flat
		;
	quit;
	
	/* Соединяем в единый справочник ПБО */
	data public.product_lvl_all;
		set public.lvl5 public.lvl4 public.lvl3 public.lvl2 public.lvl1;
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
				(t1.END_DT - t1.START_DT + 1) as promo_lifetime,
				t1.CHANNEL_CD,
				t1.NP_GIFT_PRICE_AMT,
				t1.PROMO_GROUP_ID,
				compress(promo_mechanics,'', 'ak') as promo_mechanics_name,
				1 as promo_flag		
			from
				&promo_lib..&ia_promo. as t1
			left join
				public.ia_promo_x_pbo_leaf as t2
			on 
				t1.PROMO_ID = t2.PROMO_ID
			where
				&filter.
				and t1.start_dt >= &hist_start_dt.
		;
	quit;

		
	/* Расшиваем интервалы по дням */
	data public.na_abt1;
		set public.promo_skelet;
		format sales_dt DATE9.;
		do sales_dt=start_dt to end_dt;
			output;
		end;
	run;
	
	/* Сохраняем связующие таблицы для скоринга */
	data nac.pbo_lvl_all;
		set public.pbo_lvl_all;
	run;

	data nac.product_lvl_all;
		set public.product_lvl_all;
	run;

	/* Удаляем промежуточные таблицы */
	proc casutil;
		droptable casdata="ia_pbo_loc_hierarchy" incaslib="public" quiet;
		droptable casdata="ia_product_hierarchy" incaslib="public" quiet;
		droptable casdata="pbo_hier_flat" incaslib="public" quiet;
		droptable casdata="product_hier_flat" incaslib="public" quiet;
		droptable casdata="lvl5" incaslib="public" quiet;
		droptable casdata="lvl4" incaslib="public" quiet;
		droptable casdata="lvl3" incaslib="public" quiet;
		droptable casdata="lvl2" incaslib="public" quiet;
		droptable casdata="lvl1" incaslib="public" quiet;
		droptable casdata="pbo_lvl_all" incaslib="public" quiet;
		droptable casdata="product_lvl_all" incaslib="public" quiet;
		droptable casdata="promo_skelet" incaslib="public" quiet;
	run;
	

	/****** 2. One hot кодировка механики промо акции ******/
	
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
	
	/* Транспонируем механику промо в вектор */
	proc cas;
	transpose.transpose /
		table = {
			name="promo_mechanics",
			caslib="public",
			groupby={"promo_id"}}
		transpose={"promo_flag"} 
		id={"promo_mechanics_name"} 
		casout={name="promo_mechanics_one_hot", caslib="public", replace=true};
	quit;
	
	/* Заменяем пропуски на нули */
	data public.promo_mechanics_one_hot_zero;
		set public.promo_mechanics_one_hot;
		drop _name_;
		array change _numeric_;
	    	do over change;
	            if change=. then change=0;
	        end;
	run ;
	
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
				t1.promo_mechanics_name,
				t2.*
			from
				public.na_abt1 as t1
			left join
				public.promo_mechanics_one_hot_zero as t2
			on
				t1.promo_id = t2.promo_id
		;
	quit;

	
	proc casutil;
		droptable casdata="na_abt1" incaslib="public" quiet;
		droptable casdata="promo_mechanics" incaslib="public" quiet;
		droptable casdata="promo_mechanics_one_hot" incaslib="public" quiet;
		droptable casdata="promo_mechanics_one_hot_zero" incaslib="public" quiet;
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
	proc casutil;
		load data=etl_ia.product(
			where=(
				&ETL_CURRENT_DTTM. <= valid_to_dttm and
				&ETL_CURRENT_DTTM. >= valid_from_dttm
			)
		) casout='ia_product' outcaslib='public' replace;
		
		load data=etl_ia.product_HIERARCHY(
			where=(
				&ETL_CURRENT_DTTM. <= valid_to_dttm and
				&ETL_CURRENT_DTTM. >= valid_from_dttm
			)
		) casout='IA_product_HIERARCHY' outcaslib='public' replace;
		
		load data=etl_ia.product_ATTRIBUTES(
			where=(
				&ETL_CURRENT_DTTM. <= valid_to_dttm and
				&ETL_CURRENT_DTTM. >= valid_from_dttm
			)
		) casout='IA_product_ATTRIBUTES' outcaslib='public' replace;
	run;
	  
	proc cas;
	transpose.transpose /
	   table={name="ia_product_attributes", caslib="public", groupby={"product_id"}} 
	   attributes={{name="product_id"}} 
	   transpose={"PRODUCT_ATTR_VALUE"} 
	   prefix="A_" 
	   id={"PRODUCT_ATTR_NM"} 
	   casout={name="attr_transposed", caslib="public", replace=true};
	quit;
	
	proc fedsql sessref=casauto;
		create table public.product_hier_flat{options replace=true} as
			select 
				t1.product_id, 
				t2.product_id  as LVL4_ID,
				t3.product_id  as LVL3_ID,
				t3.PARENT_product_id as LVL2_ID, 
				1 as LVL1_ID
			from 
				(select * from public.ia_product_hierarchy where product_lvl=5) as t1
			left join 
				(select * from public.ia_product_hierarchy where product_lvl=4) as t2
			on 
				t1.PARENT_PRODUCT_ID=t2.PRODUCT_ID
			left join 
				(select * from public.ia_product_hierarchy where product_lvl=3) as t3
			on
				t2.PARENT_PRODUCT_ID=t3.PRODUCT_ID
	 	;
	quit;
	
	proc fedsql sessref=casauto;
		create table public.product_dictionary_ml{options replace=true} as
			select 
				t1.product_id, 
				coalesce(t1.lvl4_id,-9999) as prod_lvl4_id,
				coalesce(t1.lvl3_id,-999) as prod_lvl3_id,
				coalesce(t1.lvl2_id,-99) as prod_lvl2_id,
				coalesce(t15.product_nm,'NA') as product_nm,
				coalesce(t14.product_nm,'NA') as prod_lvl4_nm,
				coalesce(t13.product_nm,'NA') as prod_lvl3_nm,
				coalesce(t12.product_nm,'NA') as prod_lvl2_nm,
				compress(coalesce(t12.product_nm,'NA'),'', 'ak') as prod_lvl2_name,
				t3.A_HERO,
				t3.A_ITEM_SIZE,
				t3.A_OFFER_TYPE,
				t3.A_PRICE_TIER
			from
				public.product_hier_flat as t1
			left join
				public.attr_transposed as t3
			on
				t1.product_id = t3.product_id
			left join
				PUBLIC.IA_product as t15
			on
				t1.product_id = t15.product_id
			left join
				PUBLIC.IA_product as t14
			on
				t1.lvl4_id = t14.product_id
			left join
				PUBLIC.IA_product as t13
			on
				t1.lvl3_id = t13.product_id
			left join
				PUBLIC.IA_product as t12
			on
				t1.lvl2_id = t12.product_id
		;
	quit;

	/* Копируем таблицу product_dictionary_ml в nac */
	data nac.product_dictionary_ml;
		set public.product_dictionary_ml;
		category_name = translate(trim(prod_lvl2_nm),'_',' ', '_', '&', '_', '-');
	run;
	
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
		droptable casdata="attr_transposed" incaslib="public" quiet;
		droptable casdata="product_hier_flat" incaslib="public" quiet;
		droptable casdata="IA_product" incaslib="public" quiet;
		droptable casdata="IA_product_HIERARCHY" incaslib="public" quiet;
		droptable casdata="IA_product_ATTRIBUTES" incaslib="public" quiet;
		droptable casdata="na_abt3" incaslib="public" quiet;
	run;

	/****** 6. Атрибуты ПБО ******/
	proc casutil;	
		load data=etl_ia.pbo_location(
				where=(
					&ETL_CURRENT_DTTM. <= valid_to_dttm and
					&ETL_CURRENT_DTTM. >= valid_from_dttm
				)
			) casout='ia_pbo_location' outcaslib='public' replace;
	
		load data=etl_ia.PBO_LOC_HIERARCHY(
				where=(
					&ETL_CURRENT_DTTM. <= valid_to_dttm and
					&ETL_CURRENT_DTTM. >= valid_from_dttm
				)
			) casout='ia_pbo_loc_hierarchy' outcaslib='public' replace;
	
		load data=etl_ia.PBO_LOC_ATTRIBUTES(
				where=(
					&ETL_CURRENT_DTTM. <= valid_to_dttm and
					&ETL_CURRENT_DTTM. >= valid_from_dttm
				)
			) casout='ia_pbo_loc_attributes' outcaslib='public' replace;
	run;
	
	proc cas;
	transpose.transpose /
	   table={name="ia_pbo_loc_attributes", caslib="public", groupby={"pbo_location_id"}} 
	   attributes={{name="pbo_location_id"}} 
	   transpose={"PBO_LOC_ATTR_VALUE"} 
	   prefix="A_" 
	   id={"PBO_LOC_ATTR_NM"} 
	   casout={name="attr_transposed", caslib="public", replace=true};
	quit;
	
	proc fedsql sessref=casauto;
	   create table public.pbo_hier_flat{options replace=true} as
			select t1.pbo_location_id, 
				   t2.PBO_LOCATION_ID as LVL3_ID,
				   t2.PARENT_PBO_LOCATION_ID as LVL2_ID, 
				   1 as LVL1_ID
			from 
			(select * from public.ia_pbo_loc_hierarchy where pbo_location_lvl=4) as t1
			left join 
			(select * from public.ia_pbo_loc_hierarchy where pbo_location_lvl=3) as t2
			on t1.PARENT_PBO_LOCATION_ID=t2.PBO_LOCATION_ID
	 		;
	quit;
	
	proc fedsql sessref=casauto;
		create table public.pbo_dictionary_ml{options replace=true} as
			select 
				t2.pbo_location_id, 
				coalesce(t2.lvl3_id,-999) as lvl3_id,
				coalesce(t2.lvl2_id,-99) as lvl2_id,
				coalesce(t14.pbo_location_nm,'NA') as pbo_location_nm,
				coalesce(t13.pbo_location_nm,'NA') as lvl3_nm,
				coalesce(t12.pbo_location_nm,'NA') as lvl2_nm,
				t3.A_AGREEMENT_TYPE,
				t3.A_BREAKFAST,
				t3.A_BUILDING_TYPE,
				t3.A_COMPANY,
				t3.A_DELIVERY,
				t3.A_DRIVE_THRU,
				t3.A_MCCAFE_TYPE,
				t3.A_PRICE_LEVEL,
				t3.A_WINDOW_TYPE
			from 
				public.pbo_hier_flat t2
			left join
				public.attr_transposed t3
			on
				t2.pbo_location_id=t3.pbo_location_id
			left join
				PUBLIC.IA_PBO_LOCATION t14
			on 
				t2.pbo_location_id=t14.pbo_location_id
			left join
				PUBLIC.IA_PBO_LOCATION t13
			on 
				t2.lvl3_id=t13.pbo_location_id
			left join
				PUBLIC.IA_PBO_LOCATION t12
			on
				t2.lvl2_id=t12.pbo_location_id;
	quit;
	
	/* Перекодировка текстовых переменных. */
	%macro text_encoding(table, variable);
		/*
		Параметры:
			table : таблица в которой хотим заненить текстовую переменную
			variable : название текстовой переменной
		Выход:
			* Таблица table с дополнительным столбцом variable_id
			* Таблица encoding_variable с привозкой id к старым значениям
		*/
		proc casutil;
	 		droptable casdata="encoding_&variable." incaslib="public" quiet;
	 	run;
	
		proc fedsql sessref=casauto;
			create table public.unique{options replace=true} as
				select distinct
					&variable
				from
					&table. 
				;
		quit;
	
		data work.unique;
			set public.unique;
		run;
	
		data nac.encoding_&variable.;
			set work.unique;
			&variable._id = _N_;
		run;
	
		data public.encoding_&variable.;
			set nac.encoding_&variable.;
		run;
	
		proc fedsql sessref = casauto;
			create table public.&table.{options replace=true} as 
				select
					t1.*,
					t2.&variable._id
				from
					&table. as t1
				left join
					public.encoding_&variable. as t2
				on
					t1.&variable = t2.&variable
			;
		quit;
	
	%mend;
	
	%text_encoding(public.pbo_dictionary_ml, A_AGREEMENT_TYPE);
	%text_encoding(public.pbo_dictionary_ml, A_BREAKFAST);
	%text_encoding(public.pbo_dictionary_ml, A_BUILDING_TYPE);
	%text_encoding(public.pbo_dictionary_ml, A_COMPANY);
	%text_encoding(public.pbo_dictionary_ml, A_DELIVERY);
	%text_encoding(public.pbo_dictionary_ml, A_MCCAFE_TYPE);
	%text_encoding(public.pbo_dictionary_ml, A_PRICE_LEVEL);
	%text_encoding(public.pbo_dictionary_ml, A_DRIVE_THRU);
	%text_encoding(public.pbo_dictionary_ml, A_WINDOW_TYPE);
	
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

	/* Копируем таблицу pbo_dictionary_ml в nac */
	data nac.pbo_dictionary_ml;
		set public.pbo_dictionary_ml;
	run;
	
	proc casutil;
		droptable casdata="na_abt4" incaslib="public" quiet;
		droptable casdata="attr_transposed" incaslib="public" quiet;
		droptable casdata="pbo_hier_flat" incaslib="public" quiet;
		droptable casdata="pbo_dictionary_ml" incaslib="public" quiet;
		droptable casdata='ia_pbo_location' incaslib='public' quiet;
		droptable casdata='IA_PBO_LOC_HIERARCHY' incaslib='public' quiet;
		droptable casdata='IA_PBO_LOC_ATTRIBUTES' incaslib='public' quiet;
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
	proc casutil;
		load data=etl_ia.pbo_sales(
			where=(
				&ETL_CURRENT_DTTM. <= valid_to_dttm and
				&ETL_CURRENT_DTTM. >= valid_from_dttm
			)
		) casout='ia_pbo_sales_history' outcaslib='public' replace;
	run;
	
	/* Агрегируем чеки до ПБО, год, месяц, день недели */
	proc fedsql sessref=casauto;
		create table public.gc_aggr_smart{options replace=true} as
			select
				t1.pbo_location_id,
				t1.year,
				t1.month,
				t1.weekday,
				mean(t1.receipt_qty) as mean_receipt_qty,
				std(t1.receipt_qty) as std_receipt_qty
			from (
				select
					pbo_location_id,
					year(sales_dt) as year,
					month(sales_dt) as month,
					weekday(sales_dt) as weekday,
					receipt_qty
				from
					public.ia_pbo_sales_history
				where
					channel_cd = 'ALL'
			) as t1
			group by
				t1.pbo_location_id,
				t1.year,
				t1.month,
				t1.weekday			
		;
	quit;
	
	/* Агрегируем чеки до год, месяц, день недели */
	proc fedsql sessref=casauto;
		create table public.gc_aggr_dump{options replace=true} as
			select
				t1.year,
				t1.month,
				t1.weekday,
				mean(t1.receipt_qty) as mean_receipt_qty,
				std(t1.receipt_qty) as std_receipt_qty
			from (
				select
					year(sales_dt) as year,
					month(sales_dt) as month,
					weekday(sales_dt) as weekday,
					receipt_qty
				from
					public.ia_pbo_sales_history
				where
					channel_cd = 'ALL'
			) as t1
			group by
				t1.year,
				t1.month,
				t1.weekday			
		;
	quit;

	/* Сохраняем таблицы для сборки скоринговой выборки */
	data nac.gc_aggr_smart;
		set public.gc_aggr_smart;
	run;

	data nac.gc_aggr_dump;
		set public.gc_aggr_dump;
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
		droptable casdata="ia_pbo_sales_history" incaslib="public" quiet;	
	run;


	/****** 9. Признаки описывающие продажи промо товаров ******/

	/* Создаем временные ряды продаж мастеркодов */
	proc sql;
		create table nac.pmix_mastercode_sum as
			select
				t1.pbo_location_id,
				t1.PROD_LVL4_ID,
				t1.sales_dt,
				sum(t1.sales_qty) as sales_qty
			from (
				select
					t2.PROD_LVL4_ID,
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
						&ETL_CURRENT_DTTM. >= valid_from_dttm

				) as t1
				inner join
					nac.product_dictionary_ml as t2
				on
					t1.product_id = t2.product_id
				where
					t1.channel_cd = 'ALL'
			) as t1
			group by
				t1.pbo_location_id,
				t1.PROD_LVL4_ID,
				t1.sales_dt
		;
	quit;

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
			where
				&filter.
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
	
	/* Добавляем к витрине характеристики трафика ресторана */
	proc fedsql sessref=casauto;
		create table public.na_abt8{options replace=true} as
			select
				t1.*,
				coalesce(t2.mean_sales_qty, t3.mean_sales_qty) as mean_sales_qty,
				coalesce(t2.std_sales_qty, t3.std_sales_qty) as std_sales_qty
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
	run;

	/****** 10. Добавление целевой переменной ******/
	
	/* Меняем ID */
	proc sql;
		create table work.na_calculation_result as
			select
				t1.promo_id,
				t2.pbo_location_id,
				t1.sales_dt,
				t1.n_a,
				t1.t_a
			from
				nac.na_calculation_result as t1
			inner join (
				select distinct
					PBO_LOCATION_ID,
					input(PBO_LOC_ATTR_VALUE, best32.) as store_id
				from
					etl_ia.pbo_loc_attributes
				where
					PBO_LOC_ATTR_NM='STORE_ID' and
					&ETL_CURRENT_DTTM. <= valid_to_dttm and
					&ETL_CURRENT_DTTM. >= valid_from_dttm
			) as t2
			on
				t1.PBO_LOCATION_ID = t2.store_id
		;
	quit;
	
	/* Выгружаем в cas таблицу */
	data public.na_calculation_result;
		set work.na_calculation_result;
	run;
	
	/* Удаляем таблицу */
	proc casutil;
		droptable casdata="na_train" incaslib="public" quiet;		
	run;

	proc fedsql sessref=casauto;
		create table public.na_train{options replace=true} as
			select
				t1.*,
				t2.n_a,
				t2.t_a	
			from
				public.na_abt8 as t1
			inner join
				public.na_calculation_result as t2
			on
				t1.promo_id = t2.promo_id and
				t1.pbo_location_id = t2.pbo_location_id and
				t1.sales_dt = t2.sales_dt
		;
	quit;
	
	proc casutil;
		droptable casdata="na_abt8" incaslib="public" quiet;	
		promote casdata="na_train" incaslib="public" outcaslib="public";
	run;
	
	/* Дополнительно сохраняем витрину в Nac */
	data nac.na_train;
		set public.na_train;
	run;

%mend;
