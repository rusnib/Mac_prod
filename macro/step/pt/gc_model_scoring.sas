%macro gc_scoring_builing(
		data = nac.promo_effectivness_ta_predict,
		promo_lib = nac,
		num_of_changepoint = 10,
		history_end = '1apr2021'd
	);
/* 
	Макрос собирает скоринговую витрину для модели
	декомпозиции GC.
	Параметры:
	----------
		* data : Таблица с прогнозом от модели t_a.
		* promo_lib : Директория, где лежат нормировочные константы
			для промо и даты начала временных рядов.
		* num_of_changepoint : Число точек смены тренда 
			(должно совпадать с числом при обучении).
		* history_end : конец истории, который был указан в обучении.
*/
	/* Считаем t (номер наблюдения во временном ряду) и группируем промо механики */
	proc sql;
		create table work.gc_scoring1 as
			select
				t1.p_t_a,
				t1.promo_id,
				t1.pbo_location_id,
				t1.sales_dt,
				t2.time_series_start,
				(t1.sales_dt - t2.time_series_start + 1) as t,
				sum(
					ProductnewlaunchPermanentinclite,
					Productlineextension,
					Productrehitsameproductnolineext,
					ProductnewlaunchLTO,
					NPPromoSupport
				) * t1.p_t_a as new_launch,
				sum(
					Temppricereductiondiscount,
					Discount
				) * t1.p_t_a as discount,
				sum(
					Pairsdifferentcategories,
					Pairs
				) * t1.p_t_a as pairs,
				EVMSet * t1.p_t_a as evm_set,
				bundle * t1.p_t_a as bundle,
				sum(
					GiftforpurchaseNonProduct,
					Giftforpurchaseforproduct,
					GiftforpurchaseSampling
				) * t1.p_t_a as gift_for_purchase,
				undefined * t1.p_t_a as undefined
			from
				&data. as t1
			inner join
				&promo_lib..time_series_start_date as t2
			on
				t1.pbo_location_id = t2.pbo_location_id
		;
	quit;
	
	/* Добавляем коэффциенты недельной и годовой сезонности  */
	data work.gc_scoring2 (drop=pi i);
		set work.gc_scoring1;
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
	data work.gc_scoring3 (drop=i);
		set work.gc_scoring2;
		array s[&num_of_changepoint.];
		array b[&num_of_changepoint.];
		do i=1 to &num_of_changepoint.;
			s[i] = round(0.7 * (&history_end. - time_series_start + 1) / &num_of_changepoint. * i);
			b[i] = (t >= s[i]) * (t - s[i]);
		end;
	run;
	
	/* Добавляем погоду */
	proc sql;
		create table work.gc_scoring4 as
			select
				t1.*,
				t2.temperature,
				t2.precipitation	
			from
				work.gc_scoring3 as t1
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
	
	/* добавляем нормировочные константы для промо и погоды */
	proc sql;
		create table work.gc_scoring5 as
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
				work.gc_scoring4 as t1
			inner join
				&promo_lib..promo_max as t2
			on
				t1.pbo_location_id = t2.pbo_location_id
		;
	quit;
	
	/* Делим промо признаки на максимальное значение  */
	data work.gc_scoring6 (drop=max_new_launch max_discount max_pairs
			 max_evm_set max_bundle max_gift_for_purchase max_undefined
			 mean_temperature mean_precipitation std_temperature std_precipitation);
		set work.gc_scoring5;
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

	proc datasets library=work nolist;
		delete gc_scoring1;
		delete gc_scoring2;	
		delete gc_scoring3;		
		delete gc_scoring4;			
		delete gc_scoring5;			
	run;

%mend;


%macro gc_predict(
		data = work.gc_scoring6,
		out = nac.gc_prediction,
		num_of_changepoint = 10,
		posterior_samples = nac.gc_out_train,
		train_target_max = nac.receipt_qty_max
	);
	/* 
		Макрос прогнозирует целевую переменную

		Входные параметры:
		------------------
			data : Скоринговая выборка
			out : Название результирующей таблицы с прогнозом
			* num_of_changepoint : Число точек смены тренда 
				(должно совпадать с числом при обучении).
			posterior_samples : Таблица с сэмплами из апостериорного распределения
			train_target_max : Таблица с максимальными значениями целевой переменной
				на обучающей выборке
 
	*/

	/* Усредняем коэффициенты модели */
	proc means data=&posterior_samples.  mean noprint;
		by pbo_location_id;
		output out=work.mean_out_train(where=(_stat_='MEAN'));
	run;

	/* Добавляем коэффициенты к витрине */
	proc sql;
		create table work.scoring as
			select
				t1.*,
				t2.*
			from
				&data. as t1
			inner join
				work.mean_out_train as t2
			on
				t1.pbo_location_id = t2.pbo_location_id
		;
	quit;
	
	/* Вычисляем прогноз */
	data work.scoring2;
		set work.scoring;
	
		array season_data[26] 
			week_season_cos1-week_season_cos3
			week_season_sin1-week_season_sin3 
			year_season_cos1-year_season_cos10
			year_season_sin1-year_season_sin10;
		array beta[26] beta1-beta26;
		array b[&num_of_changepoint.];
		array delta[&num_of_changepoint.];
		array alpha[7] alpha1-alpha7;
		array promo_data[7] 
			new_launch
			discount
			pairs
			evm_set
			bundle
			gift_for_purchase
			undefined;
			
		predict = 0;
		promo = 0;
		season = 0;
		weather = w1*temperature + w2*precipitation; 
		trend = t*k + m;
		do i = 1 to &num_of_changepoint.;
			trend + delta[i]*b[i];
		end;
		do i = 1 to 26;
			season + season_data[i]*beta[i];
		end;
		do i=1 to 7;
			promo + alpha[i]*promo_data[i];
		end;
		regular = season + trend + weather;
		predict = season + trend + promo + weather;
	run;

	/* Умножаем на нормировку */
	proc sql;
		create table &out. as
			select
				t1.promo_id,
				t1.pbo_location_id,
				t1.sales_dt,
				t1.predict * t2.max_receipt_qty as predict,
				t1.trend * t2.max_receipt_qty as trend,
				t1.promo * t2.max_receipt_qty as promo,
				t1.regular * t2.max_receipt_qty as regular,
				t1.weather * t2.max_receipt_qty as weather
			from 
				work.scoring2 as t1
			inner join
				&train_target_max. as t2
			on
				t1.pbo_location_id = t2.pbo_location_id
		;
	quit;

%mend;
