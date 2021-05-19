%macro upt_fit(
		train = nac.upt_train,
		upt_promo_max = nac.upt_train_max,
		upt_est = nac.upt_parameters
	);

	/* 
		Скрипт обучает модель ridge регрессии для разложения upt на промо
			эффекты. На вход поступает таблица train,
			 на выходе таблица с параметрами модели.
	
		Входные параметры:
		------------------
			train : Обучающая выборка
			upt_promo_max : Таблица с нормировочными константами
			upt_est : Таблица, в которую сохраняются параметры модели
		
	*/
	
	
	/* нормируем промо признаки, целевую переменную и сохраняем всё в отдельную таблицу */
	proc sql;
		create table work.upt_train_max as
			select
				t1.*,
				max(t1.upt) as max_upt,
				max(t1.t) as max_t,
				max(t1.positive_promo_na) as max_positive_promo_na,
				max(t1.mastercode_promo_na) as max_mastercode_promo_na,
				max(t1.Undefined_Product_Group) as max_Undefined_Product_Group,
				max(t1.Cold_Drinks) as max_Cold_Drinks,
				max(t1.Hot_Drinks) as max_Hot_Drinks,
				max(t1.Breakfast) as max_Breakfast,
				max(t1.Condiments) as max_Condiments,
				max(t1.Desserts) as max_Desserts,
				max(t1.Fries) as max_Fries,
				max(t1.Starters___Salad) as max_Starters___Salad,
				max(t1.SN_CORE) as max_SN_CORE,
				max(t1.McCafe) as max_McCafe,
				max(t1.Non_product) as max_Non_product,
				max(t1.SN_EDAP) as max_SN_EDAP,
				max(t1.SN_PREMIUM) as max_SN_PREMIUM,
				max(t1.Value_Meal) as max_Value_Meal,
				max(t1.Nuggets) as max_Nuggets,
				max(t1.Shakes) as max_Shakes
			from
				&train. as t1
			group by
				t1.product_id
		;
		
		create table &upt_promo_max. as
			select
				t1.product_id,
				max(t1.upt) as max_upt,
				max(t1.t) as max_t,
				max(t1.positive_promo_na) as max_positive_promo_na,
				max(t1.mastercode_promo_na) as max_mastercode_promo_na,
				max(t1.Undefined_Product_Group) as max_Undefined_Product_Group,
				max(t1.Cold_Drinks) as max_Cold_Drinks,
				max(t1.Hot_Drinks) as max_Hot_Drinks,
				max(t1.Breakfast) as max_Breakfast,
				max(t1.Condiments) as max_Condiments,
				max(t1.Desserts) as max_Desserts,
				max(t1.Fries) as max_Fries,
				max(t1.Starters___Salad) as max_Starters___Salad,
				max(t1.SN_CORE) as max_SN_CORE,
				max(t1.McCafe) as max_McCafe,
				max(t1.Non_product) as max_Non_product,
				max(t1.SN_EDAP) as max_SN_EDAP,
				max(t1.SN_PREMIUM) as max_SN_PREMIUM,
				max(t1.Value_Meal) as max_Value_Meal,
				max(t1.Nuggets) as max_Nuggets,
				max(t1.Shakes) as max_Shakes
			from
				&train. as t1
			group by
				t1.product_id
		;
	quit;
	
	/* Делим промо признаки, целевую переменную на максимальное значение  */
	data work.upt_train_n (drop = max_upt max_t max_positive_promo_na max_mastercode_promo_na
		max_Undefined_Product_Group max_Cold_Drinks max_Hot_Drinks max_Breakfast
		max_Condiments max_Desserts max_Fries max_Starters___Salad max_SN_CORE
		max_McCafe max_Non_product max_SN_EDAP max_SN_PREMIUM max_Value_Meal
		max_Nuggets max_Shakes) ;
		set work.upt_train_max;
		
		upt = coalesce(divide(upt, max_upt), 0);
		t = coalesce(divide(t, max_t), 0);
		positive_promo_na = coalesce(divide(positive_promo_na, max_positive_promo_na), 0);
		mastercode_promo_na = coalesce(divide(mastercode_promo_na, max_mastercode_promo_na), 0);
		Undefined_Product_Group = coalesce(divide(Undefined_Product_Group, max_Undefined_Product_Group), 0);
		Cold_Drinks = coalesce(divide(Cold_Drinks, max_Cold_Drinks), 0);
		Hot_Drinks = coalesce(divide(Hot_Drinks, max_Hot_Drinks), 0);
		Breakfast = coalesce(divide(Breakfast, max_Breakfast), 0);
		Condiments = coalesce(divide(Condiments, max_Condiments), 0);
		Desserts = coalesce(divide(Desserts, max_Desserts), 0);
		Fries = coalesce(divide(Fries, max_Fries), 0);
		Starters___Salad = coalesce(divide(Starters___Salad, max_Starters___Salad), 0);
		SN_CORE = coalesce(divide(SN_CORE, max_SN_CORE), 0);
		McCafe = coalesce(divide(McCafe, max_McCafe), 0);
		Non_product = coalesce(divide(Non_product, max_Non_product), 0);
		SN_EDAP = coalesce(divide(SN_EDAP, max_SN_EDAP), 0);
		SN_PREMIUM = coalesce(divide(SN_PREMIUM, max_SN_PREMIUM), 0);
		Value_Meal = coalesce(divide(Value_Meal, max_Value_Meal), 0);
		Nuggets = coalesce(divide(Nuggets, max_Nuggets), 0);
		Shakes = coalesce(divide(Shakes, max_Shakes), 0);
	run;

	options nomlogic nomprint nosymbolgen nosource nonotes;
		
	proc reg data=work.upt_train_n outest=&upt_est. ridge=0.1 noprint;
		model upt = t positive_promo_na mastercode_promo_na
		Undefined_Product_Group Cold_Drinks Hot_Drinks Breakfast
		Condiments Desserts Fries Starters___Salad SN_CORE
		McCafe Non_product SN_EDAP SN_PREMIUM Value_Meal
		Nuggets Shakes;
		by product_id;
	run;
	
	options mlogic mprint symbolgen source notes;

%mend;

