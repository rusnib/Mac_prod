%macro predict(
		data = work.test_n2,
		out = work.scoring3,
		posterior_samples = tmp.out_train,
		train_target_max = work.receipt_qty_max
	);
	/* 
		Макрос прогнозирует целевую переменную

		Входные параметры:
		------------------
			data : Скоринговая выборка
			out : Название результирующей таблицы с прогнозом
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
		array alpha[6] alpha1-alpha6;
		array promo_data[6] 
			np_promo_support
			discount
			pairs
			evm
			bogo
			non_product_gift;
	
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
		do i=1 to 6;
			promo + alpha[i]*promo_data[i];
		end;
		regular = season + trend + weather;
		predict = season + trend + promo + weather;
	run;

	/* Усредняем прогноз по всем семплам и умножаем на нормировку */
	proc sql;
		create table &out. as
			select
				t1.pbo_location_id,
				t1.sales_dt,
				t1.receipt_qty,
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