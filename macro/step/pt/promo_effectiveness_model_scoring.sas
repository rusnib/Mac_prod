/****** Скоринг ******/
proc casutil;
    droptable casdata="russca_npf_scoring_pred" incaslib="public" quiet;
run;


proc astore;
  score data=public.russca_npf_scoring
  copyvars=(_all_)
  rstore=public.russca_models_na
  out=public.russca_npf_scoring_pred;
quit;

proc casutil;
    promote casdata="russca_npf_scoring_pred" incaslib="public" outcaslib="public";
run;


/****** Расчет ошибки ******/

/* MAE */
proc fedsql sessref=casauto;
	select
		promo_id,
		mean(abs(n_a - P_n_a)) as mae
	from
		public.russca_npf_scoring_pred
	group by
		promo_id
	;
quit;

/* MAPE */
proc fedsql sessref=casauto;
	select
		promo_id,
		mean(divide(abs(n_a - P_n_a), n_a)) as mape,
		divide(sum(abs(n_a - P_n_a)), sum(n_a)) as wape,
		divide(sum((n_a - P_n_a)), sum(n_a)) as bias
	from
		public.russca_npf_scoring_pred
	group by
		promo_id
	;
quit;