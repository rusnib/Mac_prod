/* 
	Скрипт по созданию модели прогнозирования GC методом Prophet 
	Концепция Prophet: создаем линейную модель прогнозирования
		временного ряда, состоящую из комппонет:
	y = s_{7}(t) + s_{365}(t) + g(t) + promo
	
	* s(t) - сезонная компонента
	* g(t) - линейный тренд
	* promo - любой набор признаков промо акций, погоды и т.д.

	Модель обучается отдельно для каждого временного ряда при 
	помощи процедуры proc mcmc. Более подробно можно узнать о методе 
	в статье "Forecasting at Scale" Sean J. Taylor and Benjamin Letham

	Это вариант модели, в котором оптимальные параметры ищутся
		методом максимизации апостериорного распределения.
*/

%macro gc_prepare_abt(
		covid_start = '26mar2020'd,
		covid_end = '1jul2020'd,
		history_end = '1apr2021'd,
		num_of_changepoint = 10,
		promo_efficiency_table = na_calculation_result,
		promo_lib = nac,
		output_table = work.gc6
	);
	/*
		Макрос создает витрину для модели байессовской регрессии.

		Параметры:
		----------
			covid_start : Дата начала ковида
			covid_end : Дата конца ковида
			history_end : Последний день истории, который попадет в
				обучающую выборку
			num_of_changepoint : Число точек временного ряда,
				в которых меняется тренд
			promo_efficiency_table : Таблица с информацией об эффективности промо
			promo_lib : Бибилиотека, где лежит таблица promo_efficiency_table
			output_table : Таблица, в которую сохраняется результат
		Выход:
		------
			* Таблица output_table с витриной
			* Таблица promo_lib.time_series_start_date с датами начала временных
				рядов, чтобы в будущем собирать скоринговую выборку
	
	*/

	/* Фильтруем таблицу с фактическими GC */
	proc sql;
		create table work.gc as
			select 
				pbo_location_id,
				sales_dt,
				receipt_qty
			from 
				etl_ia.pbo_sales
			where 
				channel_cd = 'ALL' and
				sales_dt >= '1jan2018'd and
				(
					sales_dt >= &covid_end. or
					sales_dt <= &covid_start.
				) and
				(
					&ETL_CURRENT_DTTM. <= valid_to_dttm and
					&ETL_CURRENT_DTTM. >= valid_from_dttm
				)
			order by
				pbo_location_id,
				sales_dt
		;
	quit;
	
	/* Считаем дату начала каждого временного ряда */
	proc sql;
		create table &promo_lib..time_series_start_date as
			select
				pbo_location_id,
				min(sales_dt) as time_series_start
			from
				work.gc
			group by
				pbo_location_id
		;
	quit;
	
	/* Добавляем переменную t (номер наблюдения во временной ряду) */
	proc sql;
		create table work.gc2 as
			select
				t1.pbo_location_id,
				t1.sales_dt,
				t1.receipt_qty,
				(t1.sales_dt - t2.time_series_start + 1) as t,
				t2.time_series_start
			from
				work.gc as t1
			inner join
				&promo_lib..time_series_start_date as t2
			on
				t1.pbo_location_id = t2.pbo_location_id
		;
	quit;
	
	/* Добавляем коэффциенты недельной и годовой сезонности  */
	data work.gc3 (drop=pi i);
		set work.gc2;
		array week_season_cos[3];
		array week_season_sin[3];
		array year_season_cos[10];
		array year_season_sin[10];
		pi=constant("pi");
		do i = 1 to 3;
			week_season_cos[i] = cos(2*pi*i*t/7);
			week_season_sin[i] = sin(2*pi*i*t/7);	
		end;
		
		do i=1 to 10;
			year_season_cos[i] = cos(2*pi*i*t/365.25);
			year_season_sin[i] = sin(2*pi*i*t/365.25);			
		end;
	run;
	
	/* Создаем переменные для trend changepoint detection */
	data work.gc4 (drop=i);
		set work.gc3;
		array s[&num_of_changepoint.];
		array b[&num_of_changepoint.];
		do i=1 to &num_of_changepoint.;
			s[i] = round(0.7 * (&history_end. - time_series_start + 1) / &num_of_changepoint. * i);
			b[i] = (t >= s[i]) * (t - s[i]);
		end;
	run;
	
	/****** Добавляем промо ******/
	
	/* Меняем store_id на pbo_location_id в таблице c эффективностью промо */
	proc sql;
		create table work.&promo_efficiency_table.2 as
			select
				t2.pbo_location_id,
				t1.promo_id,
				t1.sales_dt,
				t1.t_a
			from
				&promo_lib..&promo_efficiency_table. as t1
			inner join (
				select
					pbo_location_id,
					input(pbo_loc_attr_value, best32.) as store_id
				from
					etl_ia.pbo_loc_attributes 
				where
					pbo_loc_attr_nm = 'STORE_ID' and
					&ETL_CURRENT_DTTM. <= valid_to_dttm and
					&ETL_CURRENT_DTTM. >= valid_from_dttm
			) as t2
			on
				t1.pbo_location_id = t2.store_id
		;
	quit;
	
	/* Добавляем механику промо акции */
	proc sql;
		create table work.&promo_efficiency_table.3 as
			select
				t1.promo_id,
				t2.promo_mechanics,
				compress(t2.promo_mechanics,'', 'ak') as promo_mechanics_name,
				t1.pbo_location_id,
				t1.sales_dt,
				t1.t_a
			from
				work.&promo_efficiency_table.2 as t1
			inner join (
				select
					*
				from
					etl_ia.promo
				where				
					&ETL_CURRENT_DTTM. <= valid_to_dttm and
					&ETL_CURRENT_DTTM. >= valid_from_dttm
			) as t2
			on
				t1.promo_id = t2.promo_id
		;
	quit;
	
	/* Сортируем таблицу для транспонирования */
	proc sort data=work.&promo_efficiency_table.3;
		by pbo_location_id sales_dt promo_id;
	run;
	
	/* Транспонируем промо механики */
	proc transpose data=work.&promo_efficiency_table.3 
		out=work.&promo_efficiency_table.3_t;
		var t_a;
		id promo_mechanics_name;
		by pbo_location_id sales_dt promo_id;
	run;
	
	/* суммируем ta от всех промо */
	proc sql;
		create table work.sum_na as
			select
				t1.pbo_location_id,
				t1.sales_dt,
				sum(ProductnewlaunchPermanentinclite) as ProductnewlaunchPermanentinclite,
				sum(Productlineextension) as Productlineextension,
				sum(Temppricereductiondiscount) as Temppricereductiondiscount,
				sum(Pairsdifferentcategories) as Pairsdifferentcategories,
				sum(EVMSet) as EVMSet,
				sum(Productrehitsameproductnolineext) as Productrehitsameproductnolineext,
				sum(ProductnewlaunchLTO) as ProductnewlaunchLTO,
				sum(Bundle) as Bundle,
				sum(GiftforpurchaseNonProduct) as GiftforpurchaseNonProduct,
				sum(Giftforpurchaseforproduct) as Giftforpurchaseforproduct,
				sum(Undefined) as Undefined,
				sum(GiftforpurchaseSampling) as GiftforpurchaseSampling,
				sum(Discount) as Discount,
				sum(Pairs) as Pairs,
				sum(NPPromoSupport) as NPPromoSupport
			from
				work.&promo_efficiency_table.3_t as t1
			group by
				t1.pbo_location_id,
				t1.sales_dt		
		;
	quit;

	/* Объединяем категории друг с другом по старой ращбивке */
	proc sql;
		create table work.sum_na2 as
			select
				pbo_location_id,
				sales_dt,
				sum(
					ProductnewlaunchPermanentinclite,
					Productlineextension,
					Productrehitsameproductnolineext,
					ProductnewlaunchLTO,
					NPPromoSupport
				) as new_launch,
				sum(
					Temppricereductiondiscount,
					Discount
				) as discount,
				sum(
					Pairsdifferentcategories,
					Pairs
				) as pairs,
				EVMSet as evm_set,
				bundle,
				sum(
					GiftforpurchaseNonProduct,
					Giftforpurchaseforproduct,
					GiftforpurchaseSampling
				) as gift_for_purchase,
				undefined
			from
				work.sum_na
		;
	quit;
	
	/* добавляем промо в витрину */
	proc sql;
		create table work.gc5 as
			select
				t1.*,
				coalesce(new_launch, 0) as new_launch,
				coalesce(discount, 0) as discount,
				coalesce(pairs, 0) as pairs,
				coalesce(evm_set, 0) as evm_set,
				coalesce(bundle, 0) as bundle,
				coalesce(gift_for_purchase, 0) as gift_for_purchase,
				coalesce(undefined, 0) as undefined
			from
				work.gc4 as t1
			left join
				work.sum_na2 as t2
			on
				t1.pbo_location_id = t2.pbo_location_id and
				t1.sales_dt = t2.sales_dt
		;
	quit;

	/* Добавляем погоду в витрину */
	proc sql;
		create table &output_table. as
			select
				t1.*,
				t2.temperature,
				t2.precipitation
			from
				work.gc5 as t1
			left join (
				select
					pbo_location_id,
					report_dt as sales_dt,
					temperature,
					precipitation
				from
					etl_ia.weather
				where
					&ETL_CURRENT_DTTM. <= valid_to_dttm and
					&ETL_CURRENT_DTTM. >= valid_from_dttm
			) as t2
			on
				t1.pbo_location_id = t2.pbo_location_id and
				t1.sales_dt = t2.sales_dt
		;
	quit;

	/* Стираем временные таблицы */
	proc datasets library=work;
		delete gc;
		delete gc2;
		delete gc3;
		delete gc4;
		delete gc5;
		delete &promo_efficiency_table.2;
		delete &promo_efficiency_table.3;
		delete &promo_efficiency_table.3_t;
		delete sum_na;
		delete sum_na2;
	run;

%mend;


%macro gc_train_test_split(
		data = work.gc6,
		history_end = '31oct2020'd,
		test_end = '30nov2020'd,
		out_train = work.train,
		out_test = work.test
	);
	/*
		Макрос делит выборку на трейн и тест. Нужен только для тестирования.

		Входные параметры:
		------------------
			data : Витрина, которую делим на трейн и тест
			history_end : Дата конца истории
			test_end : Дата конца тестовой выборки
			out_train : Название таблицы, куда сохранить обучающую выборку
			out_test : Название таблицы, куда сохранить тестовую выборку
	*/
	
	/* Делим выборку на train и test */
	proc sql;
		create table &out_train. as select * from &data. where sales_dt <= &history_end.;
		create table &out_test. as select * from &data. 
			where sales_dt >  &history_end. and sales_dt <= &test_end.;
	quit;

%mend;


%macro gc_normalize_train(
		train = work.gc6,
		out_train = nac.train_n,
		out_target_max = nac.receipt_qty_max,
		out_promo_max = nac.promo_max
	);
	/* 
		Макрос нормирует целевую переменную и промо признаки обучающей выборки
			(делением на максимальное значение во временном ряду), возвращая 
			нормированную обущающую выборку и коэффициенты нормировки для
			нормализации скоринговой выборки.
			Погодные признаки получаем вычитанием среднего и делением на стандартное
			отклонение.

		Входные параметры:
		------------------
			train : Обучающая выборка
			out_train : Название полученной нормированной выборки
			out_target_max : Таблица с максиальными значениями целевой переменной
			out_promo_max : Таблица с максимальными значениями промо
	*/

	/* Нормируем целевую переменную и сохраняем ее в отдельную таблицу */
	proc sql;
		create table work.train2 as
			select
				*,
				receipt_qty / max(receipt_qty) as y
			from
				&train.
			group by
				pbo_location_id
		;
		create table &out_target_max. as
			select
				pbo_location_id,
				max(receipt_qty) as max_receipt_qty
			from
				&train.
			group by
				pbo_location_id
		;
	quit;
	
	/* нормируем промо признаки и сохраняем их в отдельную таблицу */
	proc sql;
		create table work.train3 as
			select
				t1.*,
				max(t1.new_launch) as max_new_launch,
				max(t1.discount) as max_discount,
				max(t1.pairs) as max_pairs,
				max(t1.evm_set) as max_evm_set,
				max(t1.bundle) as max_bundle,
				max(t1.gift_for_purchase) as max_gift_for_purchase,
				max(t1.undefined) as max_undefined,
				mean(t1.temperature) as mean_temperature,
				std(t1.temperature) as std_temperature,
				mean(t1.precipitation) as mean_precipitation,
				std(t1.precipitation) as std_precipitation
			from
				work.train2 as t1
			group by
				t1.pbo_location_id
		;
		create table &out_promo_max. as
			select
				t1.pbo_location_id,
				max(t1.new_launch) as max_new_launch,
				max(t1.discount) as max_discount,
				max(t1.pairs) as max_pairs,
				max(t1.evm_set) as max_evm_set,
				max(t1.bundle) as max_bundle,
				max(t1.gift_for_purchase) as max_gift_for_purchase,
				max(t1.undefined) as max_undefined,
				mean(t1.temperature) as mean_temperature,
				std(t1.temperature) as std_temperature,
				mean(t1.precipitation) as mean_precipitation,
				std(t1.precipitation) as std_precipitation
			from
				work.train2 as t1
			group by
				t1.pbo_location_id
		;
	quit;
	
	/* Делим промо признаки на максимальное значение  */
	data &out_train. (drop=max_new_launch max_discount max_pairs
			 max_evm_set max_bundle max_gift_for_purchase max_undefined);
		set work.train3;
		new_launch = coalesce(divide(new_launch, max_new_launch), 0);
		discount = coalesce(divide(discount, max_discount), 0);
		pairs = coalesce(divide(pairs, max_pairs), 0);
		evm_set = coalesce(divide(evm_set, max_evm_set), 0);
		bundle = coalesce(divide(bundle, max_bundle), 0);
		gift_for_purchase = coalesce(divide(gift_for_purchase, max_gift_for_purchase), 0);
		undefined = coalesce(divide(undefined, max_undefined), 0);
		temperature = divide(temperature - mean_temperature, std_temperature);
		precipitation = divide(precipitation - mean_precipitation, std_precipitation);
	run;

	proc datasets library=work;
		delete train2;
		delete train3;		
	run;

%mend;


%macro gc_normalize_scoring(
		data = work.test,
		out = work.test_n2,
		train_promo_max = nac.promo_max
	);
	/* 
		Макрос нормирует промо признаки скоринговой выборки
			(делением на максимальное значение обучающего временного ряда),
			 возвращая нормированную скоринговую выборку.

		Входные параметры:
		------------------
			data : Скоринговая выборка
			out : Нормированная скоринговая выборка
			train_promo_max : Таблица с максимальными значениями промо в обучении
	*/

	/* Добавляем к скоринговой выборке нормирующие константы */
	proc sql;
		create table work.test_n as
			select
				t1.*,
				t2.max_new_launch,
				t2.max_discount,
				t2.max_pairs,
				t2.max_evm_set,
				t2.max_bundle,
				t2.max_gift_for_purchase,
				t2.max_undefined,
				t2.mean_temperature,
				t2.std_temperature,
				t2.mean_precipitation,
				t2.std_precipitation
			from
				&data. as t1
			inner join
				&train_promo_max. as t2
			on
				t1.pbo_location_id = t2.pbo_location_id
		;
	quit;
	
	/* Нормируем промо и погоду в скоринговой выборке */
	data &out.;
		set work.test_n;
		new_launch = coalesce(divide(new_launch, max_new_launch), 0);
		discount = coalesce(divide(discount, max_discount), 0);
		pairs = coalesce(divide(pairs, max_pairs), 0);
		evm_set = coalesce(divide(evm_set, max_evm_set), 0);
		bundle = coalesce(divide(bundle, max_bundle), 0);
		gift_for_purchase = coalesce(divide(gift_for_purchase, max_gift_for_purchase), 0);
		undefined = coalesce(divide(undefined, max_undefined), 0);
		temperature = divide(temperature - mean_temperature, std_temperature);
		precipitation = divide(precipitation - mean_precipitation, std_precipitation);
	run;

%mend;
