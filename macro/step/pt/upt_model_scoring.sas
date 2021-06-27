/* 
	План:
		0. Взяли спрогнозированную промо акцию. Поделили ее n_a по option number. В рамках option number пропорционально поделили по товарам.
		1. Для всех товаров, которые не в промо и не связаны с промо товарами через мастеркод мы просто:
			a. разворачиваем в промо акцию в вектор признаков:
				cat1_y | cat2_y | ... | catn_y и создаем нулевые столбцы positive_promo | mastercode
			b. джоиним с таблицей коэффициентов
				product_id | cat1_y | cat2_y | ... | catn_y | t | const | positive_promo | mastercode
			c. Джоиним с таблицей начала продаж товара и считаем sales_dt - start + 1
			d. Переменижаем коэффциенты и получаем прогноз delta и baseline
		
		Осталось множество товаров A = {учавствующих в промо} и B = {связанных с промо товарам через мастеркод}.
			Обозначим C = A U B
		
		2. Для товаров из множества С придется запустить двойной цикл:
		
			Пока промо в множестве промо акций для скоринга:
				a. Выделяем промо акцию
				b. Формируем множество С_promo товаров для этой акции
				с. Задаем пустую таблцу scoring = promo_id | pbo_location_id | sales_dt | product_id | cat1_y | cat2_y | ... | catn_y | t | const | positive_promo | mastercode
				d. Пока товар в множестве С_promo:
					* формируем скоринговую витрину по тому же принципу, как в обучении
					* добавляем полученную таблицу к scoring
				e. джоиним с таблицей коэффициентов
					product_id | cat1_y | cat2_y | ... | catn_y | t | const | positive_promo | mastercode
				f. Джоиним с таблицей начала продаж товара и считаем sales_dt - start + 1
				g. Переменижаем коэффциенты и получаем прогноз delta и baseline
*/

/* Список уникальных категорий товаров */
data work.unique_caterogy;
input category_name $40.;
datalines;
positive_promo_na
mastercode_promo_na
Undefined_Product_Group
Cold_Drinks
Hot_Drinks
Breakfast
Condiments
Desserts
Fries
Starters___Salad
SN_CORE
McCafe
Non_product
SN_EDAP
SN_PREMIUM
Value_Meal
Nuggets
Shakes
;

%macro upt_model_scoring(
	data = nac.promo_effectivness_na_predict,
	upt_promo_max = nac.upt_train_max
);
	/*
		Скрипт, который собирает скоринговую витрину для модели
			оценки влияния промо акции на upt
		Параметры:
		----------
			* data : Таблица с прогнозами n_a
			* upt_promo_max - Таблица с нормировочными константами
		Выход:
			Таблица nac.upt_scoring
	*/

	/* Выгружаем таблицу casuser.promo_prod_enh в work */
	data work.promo_prod_enh;
		set casuser.promo_prod_enh;
	run;
	
	/* Добавляем информацию о товарах в промо */
	proc sql noprint;
		create table work.upt_scoring1 as
			select
				t1.promo_id,
				t1.pbo_location_id,
				t1.sales_dt,
				t2.product_id,
				t2.option_number,
				t2.product_qty,
				t2.product_qty * t1.p_n_a as p_n_a
			from
				&data. as t1
			inner join
				work.promo_prod_enh as t2
			on
				t1.promo_id = t2.promo_id
		;
	quit;
	
	/* Считаем количество товаров в рамках каждого option number */
	proc sql noprint;
		create table work.number_of_products_per_option as
			select
				promo_id,
				option_number,
				count(distinct product_id) as cnt
			from
				work.promo_prod_enh
			group by
				promo_id,
				option_number
		;
	quit;
	
	/* Распределяем n_a равномерно по option number */
	/* Равномерно, потому что для новых товаров мы не знаем пропорции */
	proc sql noprint;
		create table work.upt_scoring2 as
			select
				t1.promo_id,
				t1.pbo_location_id,
				t1.sales_dt,
				t1.product_id,
				t1.option_number,
				t1.product_qty,
				divide(t1.p_n_a, t2.cnt) as p_n_a
			from
				work.upt_scoring1 as t1
			inner join
				work.number_of_products_per_option as t2
			on
				t1.promo_id = t2.promo_id and
				t1.option_number = t2.option_number
		;
	quit;
	
	/* Добавляем категории товаров и суммируем n_a */
	proc sql noprint;
		create table work.upt_scoring3 as
			select
				t1.promo_id,
				t1.pbo_location_id,
				t1.sales_dt,
				t2.category_name,
				sum(t1.p_n_a) as p_n_a
			from
				work.upt_scoring2 as t1
			inner join
				nac.product_dictionary_ml as t2
			on
				t1.product_id = t2.product_id
			group by
				t1.promo_id,
				t1.pbo_location_id,
				t1.sales_dt,
				t2.category_name
		;
	quit;
	
	
	/* Создаем таблицу для транспонирования */
	proc sql noprint;
		create table work.upt_scoring4 as
			select distinct
				t1.promo_id,
				t1.pbo_location_id,
				t1.sales_dt,
				trim(t2.category_name) as category_name
			from 
				&data. as t1,
				work.unique_caterogy as t2
		;
	quit;
	
	proc sql noprint;
		create table work.upt_scoring5 as
			select distinct
				t1.promo_id,
				t1.pbo_location_id,
				t1.sales_dt,
				t1.category_name,
				coalesce(t2.p_n_a, 0) as p_n_a
			from
				work.upt_scoring4 as t1
			left join
				work.upt_scoring3 as t2
			on
				t1.promo_id = t2.promo_id and
				t1.pbo_location_id = t2.pbo_location_id and
				t1.sales_dt = t2.sales_dt and
				t1.category_name = t2.category_name
		;
	quit;
	
	/* Сортируем таблицу */
	proc sort data=work.upt_scoring5;
		by pbo_location_id sales_dt promo_id;
	run;
	
	/* Транспонируем промо механики */
	proc transpose data=work.upt_scoring5 
		out=work.upt_scoring6;
		var p_n_a;
		id category_name;
		by pbo_location_id sales_dt promo_id;
	run;
	
	/* Формируем список товаров, которые не в промо и не связаны с промо товарами через мастеркод */
	
	/* Формируем список из ТОП50 товаров */
	proc sql noprint;
		create table work.sum_pmix_sales as
			select
				product_id,
				sum(sum_qty) as sum_qty
			from
				nac.aggr_pmix
			group by
				product_id
			order by
				sum_qty desc
		;
	quit;
	
	data work.regular_product(keep=product_id);
		set work.sum_pmix_sales(obs=50);
	run;
	
	/* Промо товары */
	proc sql noprint;
		create table work.a as 
			select distinct
				product_id
			from
				work.upt_scoring2
		;
	quit;
	
	/* Список товаров (либо промо товары, либо связанные с ними через мастеркод) */
	proc sql noprint;
		create table work.c as 
			select
				t1.product_id
			from
				nac.product_dictionary_ml as t1
			inner join (
				select distinct
					PROD_LVL4_ID
				from
					nac.product_dictionary_ml t1
				inner join
					work.a as t2
				on
					t1.product_id = t2.product_id
			) as t2
			on
				t1.PROD_LVL4_ID = t2.PROD_LVL4_ID
		;
	quit;
	
	/* Список регулярных товаров, кроме С */
	proc sql noprint;
		create table work.simple_regular_product as
			select
				t1.product_id
			from
				work.regular_product as t1
			left join
				work.c as t2
			on
				t1.product_id = t2.product_id
			where
				t2.product_id is missing
		;
	quit;
	
	/* Фильтруем таблицу с коэффциентами модели */
	proc sql noprint;
		create table work.simple_upt_parameters as
			select
				t1.product_id,
				t1.intercept,
				coalesce(divide(t1.t, t3.max_t), 0)  as t,
				coalesce(divide(t1.positive_promo_na, t3.max_positive_promo_na), 0) as positive_promo_na,
				coalesce(divide(t1.mastercode_promo_na, t3.max_mastercode_promo_na), 0) as mastercode_promo_na,
				coalesce(divide(t1.Undefined_Product_Group, t3.max_Undefined_Product_Group), 0) as Undefined_Product_Group,
				coalesce(divide(t1.Cold_Drinks, t3.max_Cold_Drinks), 0) as Cold_Drinks,
				coalesce(divide(t1.Hot_Drinks, t3.max_Hot_Drinks), 0) as Hot_Drinks,
				coalesce(divide(t1.Breakfast, t3.max_Breakfast), 0) as Breakfast,
				coalesce(divide(t1.Condiments, t3.max_Condiments), 0) as Condiments,
				coalesce(divide(t1.Desserts, t3.max_Desserts), 0) as Desserts,
				coalesce(divide(t1.Fries, t3.max_Fries), 0) as Fries,
				coalesce(divide(t1.Starters___Salad, t3.max_Starters___Salad), 0) as Starters___Salad,
				coalesce(divide(t1.SN_CORE, t3.max_SN_CORE), 0) as SN_CORE,
				coalesce(divide(t1.McCafe, t3.max_McCafe), 0) as McCafe,
				coalesce(divide(t1.Non_product, t3.max_Non_product), 0) as Non_product,
				coalesce(divide(t1.SN_EDAP, t3.max_SN_EDAP), 0) as SN_EDAP,
				coalesce(divide(t1.SN_PREMIUM, t3.max_SN_PREMIUM), 0) as SN_PREMIUM,
				coalesce(divide(t1.Value_Meal, t3.max_Value_Meal), 0) as Value_Meal,
				coalesce(divide(t1.Nuggets, t3.max_Nuggets), 0) as Nuggets,
				coalesce(divide(t1.Shakes, t3.max_Shakes), 0) as Shakes,		
				t3.max_upt	
			from
				nac.upt_parameters as t1
			inner join
				work.simple_regular_product as t2
			on
				t1.product_id = t2.product_id
			inner join
				&upt_promo_max. as t3
			on
				t1.product_id = t3.product_id
			where
				t1._TYPE_ = 'RIDGE'
		;
	quit;
	
	
	/* Добавляем к коэффциентам дату начала продаж товара */
	proc sql noprint;
		create table work.simple_upt_parameters2 as
			select
				t1.*,
				t2.min_date
			from
				work.simple_upt_parameters as t1
			inner join
				(select product_id, min(sales_dt) as min_date from nac.upt_train group by product_id) as t2
			on
				t1.product_id = t2.product_id
		;
	quit;
	
	
	/* Джоиним с таблицей коэффициентов */
	proc sql noprint;
		create table nac.upt_scoring as
			select
				t1.promo_id,
				t1.pbo_location_id,
				t1.sales_dt,
				t2.product_id,
				((t1.sales_dt - t2.min_date + 1) * t2.t + t2.intercept) * t2.max_upt as baseline,
				sum(
					t1.positive_promo_na * t2.positive_promo_na, 
					t1.mastercode_promo_na * t2.mastercode_promo_na,
					t1.Undefined_Product_Group * t2.Undefined_Product_Group,
					t1.Cold_Drinks * t2.Cold_Drinks,
					t1.Hot_Drinks * t2.Hot_Drinks,
					t1.Breakfast * t2.Breakfast,
					t1.Condiments * t2.Condiments,
					t1.Desserts * t2.Desserts,
					t1.Fries * t2.Fries,
					t1.Starters___Salad * t2.Starters___Salad,
					t1.SN_CORE * t2.SN_CORE,
					t1.McCafe * t2.McCafe,
					t1.Non_product * t2.Non_product,
					t1.SN_EDAP * t2.SN_EDAP,
					t1.SN_PREMIUM * t2.SN_PREMIUM,
					t1.Value_Meal * t2.Value_Meal,
					t1.Nuggets * t2.Nuggets,
					t1.Shakes * t2.Shakes
				) * t2.max_upt as delta
			from
				work.upt_scoring6 as t1,
				work.simple_upt_parameters2 as t2
		;
	quit;
	
	proc datasets library=work nolist;
		delete upt_scoring1;
		delete upt_scoring2;
		delete upt_scoring3;
		delete upt_scoring4;
		delete upt_scoring5;
		delete upt_scoring6;

		delete simple_upt_parameters2;
		delete simple_upt_parameters;

		delete unique_caterogy;
	run;	

%mend;




