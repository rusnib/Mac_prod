%macro manual_load_etl_ia;

	%fmk_load_etl_ia(mpResource=ASSORT_MATRIX);
	%fmk_load_etl_ia(mpResource=CHANNEL);
	%fmk_load_etl_ia(mpResource=COMPETITOR);
	%fmk_load_etl_ia(mpResource=COMP_MEDIA);
	%fmk_load_etl_ia(mpResource=COST_PRICE);
	%fmk_load_etl_ia(mpResource=EVENTS);
	%fmk_load_etl_ia(mpResource=INGRIDIENTS);
	%fmk_load_etl_ia(mpResource=MACRO_FACTOR);
	%fmk_load_etl_ia(mpResource=MEDIA);
	%fmk_load_etl_ia(mpResource=PBO_CLOSE_PERIOD);
	%fmk_load_etl_ia(mpResource=PBO_LOCATION);
	%fmk_load_etl_ia(mpResource=PBO_LOC_ATTRIBUTES);
	%fmk_load_etl_ia(mpResource=PBO_LOC_HIERARCHY);
	%fmk_load_etl_ia(mpResource=PRODUCT);
	%fmk_load_etl_ia(mpResource=PRODUCT_ATTRIBUTES);
	%fmk_load_etl_ia(mpResource=PRODUCT_CHAIN);
	%fmk_load_etl_ia(mpResource=PRODUCT_HIERARCHY);
	%fmk_load_etl_ia(mpResource=PROMO);
	%fmk_load_etl_ia(mpResource=PROMO_X_PBO);
	%fmk_load_etl_ia(mpResource=PROMO_X_PRODUCT);
	%fmk_load_etl_ia(mpResource=SEGMENT);
	%fmk_load_etl_ia(mpResource=WEATHER);
	%fmk_load_etl_ia(mpResource=VAT);
	/*
	%fmk_load_etl_ia_hist(mpResource=price);
	%fmk_load_etl_ia_hist(mpResource=pbo_sales);
	%fmk_load_etl_ia_hist(mpResource=pmix_sales);
	*/
%mend manual_load_etl_ia;