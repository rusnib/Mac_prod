/* 
	Обучение модели для прогнозирования n_a (t_a) 
*/

/* Гиперпараметры моделей */
%let default_hyper_params = seed=12345 loh=0 binmethod=QUANTILE 
	 maxbranch=2 
     assignmissing=useinsearch 
	 minuseinsearch=5
     ntrees=100
     maxdepth=20
     inbagfraction=0.7
     minleafsize=5
     numbin=100
     printtarget
;

%macro promo_effectiveness_model_fit(
	data = public.na_train,
	target = n_a,
	output = na_prediction_model,
	hyper_params = &default_hyper_params.
);
/* 
	Макрос обучение модель для прогнозирования n_a (t_a).
	Параметры:
	----------
		* data : Обучающий набор данных
		* target : Название целевой переменной (n_a или t_a)
		* output : Название таблицы, куда будет сохранена обученная модель
			(сохраняются в public)
		* hyper_params : Гиперпараметры модели
*/

	/* Стираем результирующие таблицы с обученными моделями */
	proc casutil;
		droptable casdata="&output." incaslib="public" quiet;
	run;
	
	/* Обучение модели */
	proc forest data=&data.
		&hyper_params.;
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
			MEAN_SALES_QTY
			STD_SALES_QTY
				/ level = interval;
		input 
			LVL3_ID
			LVL2_ID
			AGREEMENT_TYPE_ID
			BREAKFAST_ID
			BUILDING_TYPE_ID
			COMPANY_ID
			DELIVERY_ID
			DRIVE_THRU_ID
			MCCAFE_TYPE_ID
			PRICE_LEVEL_ID
			WINDOW_TYPE_ID
			 / level = nominal;
		id promo_id pbo_location_id sales_dt;
		target &target. / level = interval;
		savestate rstore=public.&output.;
		;
	run;

%mend;