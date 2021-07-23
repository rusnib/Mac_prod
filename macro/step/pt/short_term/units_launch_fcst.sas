cas casauto;
caslib _all_ assign;

proc casutil;
	droptable 
		casdata		= "ALL_ML_TRAIN_DEC" 
		incaslib	= "CASUSER" 
		quiet         
	;                 
run;                  
       
data CASUSER.ALL_ML_TRAIN_DEC(promote=yes);
	set MAX_CASL.ALL_ML_TRAIN_DEC;
	where (lvl2_id = 74  and prod_lvl2_id = 246)
	   or (lvl2_id = 486 and prod_lvl2_id = 246);
run;

proc casutil;
	droptable 
		casdata		= "ALL_ML_SCORING_DEC" 
		incaslib	= "CASUSER" 
		quiet         
	;                 
run;  

data CASUSER.ALL_ML_SCORING_DEC(promote=yes);
	set MAX_CASL.ALL_ML_SCORING_DEC;
	where (lvl2_id = 74  and prod_lvl2_id = 246)
	  or (lvl2_id = 486 and prod_lvl2_id = 246);
run;


/*/data/files/input/PMIX_MODEL_TABLE.csv */
%tech_redirect_log(mpMode=START, mpJobName=rtp_4_modeling_score_pmix, mpArea=Main);
%tech_log_event(mpMode=START, mpProcess_Nm=rtp_4_modeling_score_pmix);

%rtp_4_modeling(
		  mode					= FULL
		, external				= 1
		, ids 					= product_id pbo_location_id sales_dt
		, target				= sum_qty
		, categories			= lvl2_id prod_lvl2_id 
		, external_modeltable 	= /data/files/input/PMIX_MODEL_TABLE_BT.csv      
		, modeltable			= PMIX_MODEL_TABLE_BT				
		, traintable			= CASUSER.ALL_ML_TRAIN_DEC
		, scoretable			= CASUSER.ALL_ML_SCORING_DEC
		, resulttable			= MAX_CASL.PMIX_DAYS_RESULT_DEC
		, default_params		= seed=12345 loh=0 binmethod=QUANTILE maxbranch=2 assignmissing=useinsearch minuseinsearch=5 ntrees=10 maxdepth=10 inbagfraction=0.6 minleafsize=5 numbin=50 printtarget				
		, default_interval		= GROSS_PRICE_AMT lag_month_avg lag_month_med lag_qtr_avg lag_qtr_med lag_week_avg lag_week_med lag_month_std lag_qtr_std lag_week_std lag_month_pct10 lag_month_pct90 lag_qtr_pct10 lag_qtr_pct90 lag_week_pct10 lag_week_pct90 PRICE_RANK PRICE_INDEX				
		, default_nominal		= OTHER_PROMO SUPPORT BOGO DISCOUNT EVM_SET NON_PRODUCT_GIFT PAIRS PRODUCT_GIFT SIDE_PROMO_FLAG HERO ITEM_SIZE OFFER_TYPE PRICE_TIER AGREEMENT_TYPE BREAKFAST BUILDING_TYPE COMPANY DELIVERY DRIVE_THRU MCCAFE_TYPE PRICE_LEVEL WINDOW_TYPE week weekday month weekend_flag DEFENDER_DAY FEMALE_DAY MAY_HOLIDAY NEW_YEAR RUSSIA_DAY SCHOOL_START STUDENT_DAY SUMMER_START VALENTINE_DAY
		, model_prefix			= FOREST
	);	

%tech_redirect_log(mpMode=END, mpJobName=rtp_4_modeling_score_pmix, mpArea=Main);