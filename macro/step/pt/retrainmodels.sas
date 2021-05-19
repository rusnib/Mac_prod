/*
	Внерегламентный процесс.

	1. Инициализация окружения
	2. Рассчет эффективности промо акций на истории
		a. Подсчет из чеков n_a (число срабатываний акций) и
			 t_a(количество чеков с промо) [na_calculation.sas]
		b. Сборка витрины для модели прогнозирования n_a (t_a)
			[promo_effectiveness_abt_building.sas]
		c. Обучение модели для прогнозирования n_a (t_a)
			[promo_effectiveness_model_fitting.sas]
	3. Линейная модель для GC
		a. Сборка витрины [gc_abt_building.sas]
		b. Обучение моделей [gc_model_fitting.sas]
	4. Линейная модель для UPT
		a. Сборка витрины [upt_abt_building.sas]
		b. Обучение моделей [upt_model_fitting.sas]
*/


/*** 1. Инициализация окружения ***/
%include '/opt/sas/mcd_config/config/initialize_global.sas';
options casdatalimit=10G;

libname cheque "/data/backup/"; /* Директория с чеками */
libname nac "/data/MN_CALC"; /* Директория в которую складываем результат */

%macro assign;
	%let casauto_ok = %sysfunc(SESSFOUND ( casauto)) ;
	%if &casauto_ok = 0 %then %do; /*set all stuff only if casauto is absent */
	 cas casauto SESSOPTS=(TIMEOUT=31536000);
	 caslib _all_ assign;
	%end;
%mend;

%assign


/*** 2. Рассчет эффективности промо акций на истории ***/

/* a. Подсчет из чеков n_a и t_a */
%include '/opt/sas/mcd_config/macro/step/pt/na_calculation.sas';
%na_calculation(
	promo_lib = public, 
	ia_promo = ia_promo,
	ia_promo_x_pbo = ia_promo_x_pbo,
	ia_promo_x_product = ia_promo_x_product,
	hist_start_dt = date '2019-01-01',
	hist_end_dt =  date '2021-04-12',
	filter = channel_cd = 'ALL' and promo_id ^= 745
)

/* b. Сборка витрины для модели прогнозирования n_a и t_a */
%include '/opt/sas/mcd_config/macro/step/pt/promo_effectiveness_abt_building.sas';
%promo_effectiveness_abt_building(
	promo_lib = public, 
	ia_promo = ia_promo,
	ia_promo_x_pbo = ia_promo_x_pbo,
	ia_promo_x_product = ia_promo_x_product,
	hist_start_dt = date '2019-01-01',
	filter = t1.channel_cd = 'ALL',
	calendar_start = '01jan2017'd,
	calendar_end = '01jan2022'd
)

/* c. Обучение модели для прогнозирования n_a  и t_a */
%include '/opt/sas/mcd_config/macro/step/pt/promo_effectiveness_model_fitting.sas';
%promo_effectiveness_model_fit(
	data = public.na_train,
	target = n_a,
	output = na_prediction_model,
	hyper_params = &default_hyper_params.
)

%promo_effectiveness_model_fit(
	data = public.na_train,
	target = t_a,
	output = ta_prediction_model,
	hyper_params = &default_hyper_params.
)


/*** 3. Линейная модель для GC ***/

/* a. Сборка витрины */
%include '/opt/sas/mcd_config/macro/step/pt/gc_abt_building.sas';
%gc_prepare_abt(
	covid_start = '26mar2020'd,
	covid_end = '1jul2020'd,
	history_end = '1apr2021'd,
	num_of_changepoint = 10,
	promo_efficiency_table = na_calculation_result,
	promo_lib = nac,
	output_table = work.gc6
)

/* Нормировка промо и целевой переменной */
%gc_normalize_train(
	train = work.gc6,
	out_train = nac.train_n,
	out_target_max = nac.receipt_qty_max,
	out_promo_max = nac.promo_max
)

/* b. Обучение модели */
%include '/opt/sas/mcd_config/macro/step/pt/gc_model_fitting.sas';
%gc_fit(
	train = nac.train_n,
	posterior_samples = nac.gc_out_train,
	num_of_changepoint = 10,
	nbi_value=500,
	nmc_value=100,
	b=100,
	seed = 123	
)


/**	4. Линейная модель для UPT **/

/* a. Сборка витрины [upt_abt_building.sas] */
%include '/opt/sas/mcd_config/macro/step/pt/upt_abt_building.sas';
%upt_abt_building(
	promo_lib = public, 
	ia_promo = ia_promo,
	ia_promo_x_pbo = ia_promo_x_pbo,
	ia_promo_x_product = ia_promo_x_product,
	period_start_dt = '1jan2019'd,
	period_end_dt = '12apr2021'd
)

/* b. Обучение моделей [upt_model_fitting.sas] */
%include '/opt/sas/mcd_config/macro/step/pt/upt_model_fitting.sas';
%upt_fit(
	train = nac.upt_train,
	upt_promo_max = nac.upt_train_max,
	upt_est = nac.upt_parameters
)