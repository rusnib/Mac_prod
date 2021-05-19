%macro gc_fit(
		train = tmp.train3_n,
		posterior_samples = tmp.out_train,
		num_of_changepoint = 10,
		nbi_value=500,
		nmc_value=100,
		b=100,
		seed = 123	
	);
	/* 
		Скрипт обучает байессовскую модель. На вход поступает таблица
			train на выходе таблица c сэмплами из апостериорного распределения
			на параметры модели.
	
		Входные параметры:
		------------------
			train : Обучающая выборка
			posterior_samples : Таблица в которую сохраняются сэмплы
			num_of_changepoint : Число точек смены тренда
			nbi_value : Сколько нужно сделать "разогревочных" сэмплов
				 до начала настояшего сэмплирования
			nmc_value : Количество сэмплов из апостериорного распределения
			b : параметр априорного распределения для промо
			seed : seed для сэмплирования
		
	*/
	ods graphics on;
	proc mcmc data=&train. outpost=&posterior_samples. nbi=&nbi_value. thin=10
		nmc=&nmc_value. seed=&seed. propcov=IND;
		
		by pbo_location_id;
	
		array beta[26]; /* Коэффициенты сезонности */
		array delta[&num_of_changepoint.]; /* Коэффициенты меняющегося тренда */
		array alpha[7];	/* Коэффциенты промо */
	
		array b[&num_of_changepoint.]; /* Независимые переменные отвечающие за тренд */
		array season_data[26] 
			week_season_cos1-week_season_cos3
			week_season_sin1-week_season_sin3 
			year_season_cos1-year_season_cos10
			year_season_sin1-year_season_sin10; /* Независимые переменные сезонности */
		array promo_data[7] 
			new_launch
			discount
			pairs
			evm_set
			bundle
			gift_for_purchase
			undefined; /* Независимые переменные промо */
		
		parms m k ; /* тренд */
		parms beta:; /* сезонность */
		parms delta:; /* тренд */
		parms sigma2; /* ошибка модели */
		parms alpha:; /* промо */
		parms w1 w2; /* погода */

		prior delta: ~ laplace(loc=0, s=0.00001);
		prior beta: ~ normal(mean = 0, var = 10);
		prior alpha: ~ beta(a=1, b=&b.); 
		prior m k ~ normal(mean = 0, var = 5);
		prior sigma2 ~ igamma(shape = 3/10, scale = 10/3);
		prior w1 w2 ~ normal(mean=0, var=1e-3);
	
		call mult(season_data, beta, season);
		call mult(b, delta, trend);
		call mult(alpha, promo_data, promo_component);
		mu = m + k*t + trend + season + promo_component + w1*temperature + w2*precipitation;
	
		model y ~ n(mu, var = sigma2);
	
	run;
	ods graphics off;

%mend;