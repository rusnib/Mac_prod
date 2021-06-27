%macro rtp_7_out_integration(mpVfPmixProjName=pmix_sales_v2,
							mpVfPboProjName=pbo_sales_v1,
							mpMLPmixTabName=dm_abt.pmix_reconciled_full,
							mpInEventsMkup=dm_abt.events_mkup,
							mpInWpGc=dm_abt.wp_gc,
							mpOutPmixLt=casuser.plan_pmix_month1,
							mpOutGcLt=casuser.plan_gc_month1, 
							mpOutUptLt=casuser.plan_upt_month1, 
							mpOutPmixSt=casuser.plan_pmix_day1,
							mpOutGcSt=casuser.plan_gc_day1, 
							mpOutUptSt=casuser.plan_upt_day1, 
							mpOutOutforgc=casuser.TS_OUTFORGC,
							mpOutOutfor=casuser.TS_OUTFOR, 
							mpOutNnetWp=casuser.nnet_wp1,
							mpPrmt=Y,
							mpInLibref=&lmvInLibref.,
							mpAuth = NO);

	%if %sysfunc(sessfound(casauto))=0 %then %do;
		cas casauto;
		caslib _all_ assign;
	%end;
	
	%let pbo_table  = MN_DICT.PBO_FORECAST_RESTORED;
	%let gc_table   = MN_DICT.GC_FORECAST_RESTORED;
	
	proc cas;
		table.tableExists result = rc / caslib="mn_dict" name="NNET_WP1";
		if rc=0  then do;
			loadtable / caslib='mn_dict',
						path='NNET_WP1_ATTR.sashdat',
						casout={caslib="mn_dict" name='attr2', replace=true};
			loadtable / caslib='mn_dict',
						path='NNET_WP1.sashdat',
						casout={caslib="mn_dict" name='nnet_wp1', replace=true};
			attribute / task='ADD',
						   caslib="mn_dict",
						name='nnet_wp1',
						attrtable='attr2';
			table.promote / name="NNET_WP1" caslib="mn_dict" target="NNET_WP1" targetlib="mn_dict";
		end;
		else print("Table mn_dict.NNET_WP1 already loaded");
		
		table.tableExists result = rc / caslib="mn_dict" name="wp_gc";
		if rc=0  then do;
			loadtable / caslib='mn_dict',
			path='wp_gc.sashdat',
			casout={caslib="mn_dict" name='wp_gc', replace=true};
			table.promote / name="wp_gc" caslib="mn_dict" target="wp_gc" targetlib="mn_dict";
		end;
		else print("Table mn_dict.wp_gc already loaded");	
		
		table.tableExists result = rc / caslib="mn_long" name="events_mkup";
		if rc=0  then do;
			loadtable / caslib='mn_long',
			path='events_mkup.sashdat',
			casout={caslib="mn_long" name='events_mkup', replace=true};
			table.promote / name="events_mkup" caslib="mn_long" target="events_mkup" targetlib="mn_long";
		end;
		else print("Table mn_long.events_mkup already loaded");	
	quit;	

	%member_exists_list(mpMemberList=&mpMLPmixTabName.
								&mpInEventsMkup.
								&mpInWpGc.
								&mpOutNnetWp.
								);
								

	%local	lmvOutLibrefPmixSt 
			lmvOutTabNamePmixSt 
			lmvOutLibrefGcSt 
			lmvOutTabNameGcSt 
			lmvOutLibrefUptSt 
			lmvOutTabNameUptSt 
			lmvOutLibrefPmixLt 
			lmvOutTabNamePmixLt 
			lmvOutLibrefGcLt 
			lmvOutTabNameGcLt
			lmvOutLibrefUptLt 
			lmvOutTabNameUptLt  
			lmvOutLibrefOutforgc 
			lmvOutTabNameOutforgc 
			lmvOutLibrefOutfor 
			lmvOutTabNameOutfor 
			lmvVfPmixName
			lmvVfPmixId
			lmvVfPboName
			lmvVfPboId
			lmvInEventsMkup
			lmvInLib
			lmvReportDt
			lmvReportDttm
			lmvInLibref
			lmvAPI_URL
			;
			
	%let lmvInLib=ETL_IA;
	%let lmvReportDt=&ETL_CURRENT_DT.;
	%let lmvReportDttm=&ETL_CURRENT_DTTM.;
	%let lmvInLibref=&mpInLibref.;
	%let lmvAPI_URL = &CUR_API_URL.;
	
	%member_names (mpTable=&mpOutOutfor, mpLibrefNameKey=lmvOutLibrefOutfor, mpMemberNameKey=lmvOutTabNameOutfor);
	%member_names (mpTable=&mpOutOutforgc, mpLibrefNameKey=lmvOutLibrefOutforgc, mpMemberNameKey=lmvOutTabNameOutforgc); 
	%member_names (mpTable=&mpOutGcSt, mpLibrefNameKey=lmvOutLibrefGcSt, mpMemberNameKey=lmvOutTabNameGcSt); 
	%member_names (mpTable=&mpOutPmixSt, mpLibrefNameKey=lmvOutLibrefPmixSt, mpMemberNameKey=lmvOutTabNamePmixSt); 
	%member_names (mpTable=&mpOutUptSt, mpLibrefNameKey=lmvOutLibrefUptSt, mpMemberNameKey=lmvOutTabNameUptSt); 
	%member_names (mpTable=&mpOutGcLt, mpLibrefNameKey=lmvOutLibrefGcLt, mpMemberNameKey=lmvOutTabNameGcLt); 
	%member_names (mpTable=&mpOutPmixLt, mpLibrefNameKey=lmvOutLibrefPmixLt, mpMemberNameKey=lmvOutTabNamePmixLt); 
	%member_names (mpTable=&mpOutUptLt, mpLibrefNameKey=lmvOutLibrefUptLt, mpMemberNameKey=lmvOutTabNameUptLt); 
			
	%if &mpAuth. = YES %then %do;
		%tech_get_token(mpUsername=&SYS_ADM_USER., mpOutToken=tmp_token);
		
		filename resp TEMP;
		proc http
		  method="GET"
		  url="&lmvAPI_URL./analyticsGateway/projects?limit=99999"
		  out=resp;
		  headers 
			"Authorization"="bearer &tmp_token."
			"Accept"="application/vnd.sas.collection+json";    
		run;
		%put Response status: &SYS_PROCHTTP_STATUS_CODE;
		
		libname respjson JSON fileref=resp;
		
		data work.vf_project_list;
		  set respjson.items;
		run;
	%end;
	%else %if &mpAuth. = NO %then %do;
		%vf_get_project_list(mpOut=work.vf_project_list);
	%end;
	
	/* ���������� ID ��� VF-������� PMIX �� ��� ����� */
	%let lmvVfPmixName = &mpVfPmixProjName.;
	%let lmvVfPmixId = %vf_get_project_id_by_name(mpName=&lmvVfPmixName., mpProjList=work.vf_project_list);
	
	/* ���������� ID ��� VF-������� PBO �� ��� ����� */
	%let lmvVfPboName = &mpVfPboProjName.;
	%let lmvVfPboId = %vf_get_project_id_by_name(mpName=&lmvVfPboName., mpProjList=work.vf_project_list);
	%let lmvInEventsMkup=&mpInEventsMkup;
/* 0. �������� ������� ������ */
	%if &mpPrmt. = Y %then %do;
		proc casutil;
			droptable casdata="&lmvOutTabNameGcSt." incaslib="&lmvOutLibrefGcSt." quiet;
			droptable casdata="&lmvOutTabNamePmixSt." incaslib="&lmvOutLibrefPmixSt." quiet;
			droptable casdata="&lmvOutTabNameUptSt." incaslib="&lmvOutLibrefUptSt." quiet;
			droptable casdata="&lmvOutTabNameGcLt." incaslib="&lmvOutLibrefGcLt." quiet;
			droptable casdata="&lmvOutTabNamePmixLt." incaslib="&lmvOutLibrefPmixLt." quiet;
			droptable casdata="&lmvOutTabNameUptLt." incaslib="&lmvOutLibrefUptLt." quiet;
			droptable casdata="&lmvOutTabNameOutfor." incaslib="&lmvOutLibrefOutfor." quiet;
			droptable casdata="&lmvOutTabNameOutforgc." incaslib="&lmvOutLibrefOutforgc." quiet;
			droptable casdata="&lmvOutTabNameOutforgc." incaslib="&lmvOutLibrefOutforgc." quiet;
			*droptable casdata="pmix_sales" incaslib="&lmvInLibref." quiet;
			*droptable casdata="pmix_days_result" incaslib="&lmvInLibref." quiet;
			droptable casdata="all_ml_scoring" incaslib="&lmvInLibref." quiet;
			droptable casdata="all_ml_train" incaslib="&lmvInLibref." quiet;
		run;
	%end;
/*0.9 �������� ������ �� �������*/
	proc fedsql sessref=casauto noprint;
		create table &lmvOutLibrefOutfor..&lmvOutTabNameOutfor.{options replace=true} as
			select t1.*
					,month(cast(t1.SALES_DT as date)) as MON_START
					,month(cast(intnx('day', cast(t1.SALES_DT as date),6) as date)) as MON_END
			from "Analytics_Project_&lmvVfPmixId".horizon t1
		;
	quit;
	proc fedsql sessref=casauto noprint;
		create table &lmvOutLibrefOutforGc..&lmvOutTabNameOutforGc.{options replace=true} as
			select t1.*
					,month(cast(t1.SALES_DT as date)) as MON_START
					,month(cast(intnx('day', cast(t1.SALES_DT as date),6) as date)) as MON_END
			from "Analytics_Project_&lmvVfPboId".horizon t1
		;
	quit;
	%if &mpPrmt. = Y %then %do;
		proc casutil;
			promote casdata="&lmvOutTabNameOutfor." incaslib="&lmvOutLibrefOutfor." outcaslib="&lmvOutLibrefOutfor.";
			promote casdata="&lmvOutTabNameOutforgc." incaslib="&lmvOutLibrefOutforgc." outcaslib="&lmvOutLibrefOutforgc.";
		run;
	%end;
    

/*1. ��������� � ��������� ��������� ��������� �������*/
	%vf_apply_w_prof(&lmvOutLibrefOutfor..&lmvOutTabNameOutfor.,
					&lmvOutLibrefOutfor..&lmvOutTabNameOutforgc.,
					casuser.nnet_wp_scored1,
					casuser.daily_gc,
					&mpInEventsMkup.,
					&mpInWpGc.,
					&mpOutNnetWp.,
					&lmvInLibref.);
	
	%let lmvScoreEndDate = %sysfunc(intnx(day,&VF_HIST_END_DT_SAS.,91,s));  /* Дата окончания обучающей выборки */
	/* data casuser.pmix_daily_ ;
	  set casuser.nnet_wp_scored1; */
	data casuser.pmix_daily(drop=channel_cd_old);
      set casuser.nnet_wp_scored1(rename=(channel_cd=channel_cd_old));
      length channel_cd $48;
      channel_cd=channel_cd_old;
      where week_dt > &lmvScoreEndDate.;
	  array p_weekday{7};
	  array PR_{7};
	  keep CHANNEL_CD PBO_LOCATION_ID PRODUCT_ID period_dt mon_dt FF promo;
	  format period_dt mon_dt date9.;
	  period_dt=week_dt;
	  fc=ff;
	  if fc = . then fc = 0;
	  miss_prof=nmiss(of p_weekday:);
	  if miss_prof>0 then
		do i=1 to 7;
		p_weekday{i}=1./7.;
		end;
	  do while (period_dt<=week_dt+6);
		mon_dt=intnx('month',period_dt,0,'b');
		promo=pr_{period_dt-week_dt+1};
		ff=fc*p_weekday{period_dt-week_dt+1};
		output;
		period_dt+1;
	  end;
	run;

	proc casutil;
		droptable casdata="nnet_wp_scored1" incaslib="mn_short" quiet;
	run;
	quit;
/*
    data casuser.pmix_daily_ ;
        set casuser.pmix_daily_;
        by CHANNEL_CD PBO_LOCATION_ID PRODUCT_ID mon_dt period_dt;
        if last.period_dt then output;
    run;
*/
/*1.5 ������� ����� �������*/
  %vf_new_product(mpInCaslib=&lmvInLibref.);
/*2. ���������� ������� ������������� �������� � �������������� - � ����������� ��������������*/
	data casuser.promo_w2;
	  set casuser.promo_d; /*table from vf_apply_w_prof*/
	  format period_dt date9.;
	  do period_dt=start_DT to min(end_DT,&vf_fc_agg_end_dt_sas);
		output;
	  end;
	run;

	proc fedsql sessref=casauto;
	  create table casuser.promo_w1{options replace=true} as
	  select distinct t1.channel_cd,t1.pbo_location_id,
	  t1.product_id,t1.period_dt, 
	  cast(1 as double) as promo
	  from casuser.promo_w2 t1;
	quit;
	
	proc fedsql sessref=casauto;
        create table casuser.short_term{options replace=true} as
        select distinct t2.PBO_LOCATION_ID, t2.PRODUCT_ID, t2.sales_dt as period_dt, t3.channel_cd
            , cast(intnx('month',t2.sales_dt,0) as date) as mon_dt
                , t2.P_SUM_QTY as ff, . as promo
        from
                &mpMLPmixTabName t2 
        left join MN_DICT.ENCODING_CHANNEL_CD t3
            on t2.channel_cd=t3.channel_cd_id
        where t2.sales_dt between &VF_FC_START_DT and &VF_FC_END_SHORT_DT 
    ;
    quit;
       
   /* proc fedsql sessref=casauto;
    create table casuser.pmix_daily{options replace=true} as
        select 
            coalesce(t4.channel_cd,t1.channel_cd) as channel_cd, 
            coalesce(t4.pbo_location_id,t1.PBO_LOCATION_ID) as PBO_LOCATION_ID, 
            coalesce(t4.product_id,t1.PRODUCT_ID) as product_id,
            coalesce(t4.sales_dt,t1.period_dt) as period_dt,
            coalesce(cast(intnx('month',t4.sales_dt,0) as date),t1.mon_dt) as mon_dt,
            coalesce(t4.P_SUM_QTY,t1.ff) as ff 
        from casuser.pmix_daily_ t1 full outer join 
        (select t2.PBO_LOCATION_ID, t2.PRODUCT_ID, t2.sales_dt, t3.channel_cd
                , t2.P_SUM_QTY
                from
                &mpMLPmixTabName t2 left join MN_DICT.ENCODING_CHANNEL_CD t3
                on t2.channel_cd=t3.channel_cd_id
                where t2.sales_dt between &VF_FC_START_DT and &VF_FC_END_SHORT_DT ) t4
            on t1.PBO_LOCATION_ID=t4.PBO_LOCATION_ID and t1.PRODUCT_ID=t4.PRODUCT_ID and
            t1.period_dt = t4.sales_dt and t1.channel_cd=t4.channel_cd
   ;
   quit; */
   
   data casuser.pmix_daily(append=yes);
        set casuser.short_term;
    run;
   
   proc casutil;
			droptable casdata="short_term" incaslib="casuser" quiet;
	run;
	quit;
	
	/*2.1 TODO: ���������� ������ ��������� �������� � ���������� ���� ������*/
	data casuser.days_pbo_date_close; /*��� ����� ��� ����� ��� ������ (��������)*/
	  set &lmvInLibref..pbo_dictionary;
	  format period_dt date9.;
	  keep PBO_LOCATION_ID CHANNEL_CD period_dt;
	  CHANNEL_CD="ALL"; 
	  if A_CLOSE_DATE ne . and A_CLOSE_DATE<=&vf_fc_agg_end_dt_sas then 
	  do period_dt= max(A_CLOSE_DATE,&vf_fc_start_dt_sas) to &vf_fc_agg_end_dt_sas;
	    output;
	  end;
	run;
	
	data casuser.days_pbo_close; /*��� ����� ��� ����� �������� ������*/
	  set &lmvInLibref..PBO_CLOSE_PERIOD;
	  format period_dt date9.;
	  keep PBO_LOCATION_ID CHANNEL_CD period_dt;
	  if channel_cd="ALL" ;
	  if (end_dt>=&vf_fc_start_dt_sas and end_dt<=&vf_fc_agg_end_dt_sas) 
	  or (start_dt>=&vf_fc_start_dt_sas and start_dt<=&vf_fc_agg_end_dt_sas) 
	  or (start_dt<=&vf_fc_start_dt_sas and &vf_fc_start_dt_sas<=end_dt)
	  then
	  do period_dt=max(start_dt,&vf_fc_start_dt_sas) to min(&vf_fc_agg_end_dt_sas,end_dt);
	    output;
	  end;
	run;
	
	data casuser.days_pbo_close(append=force); /*��� ����� ������� ��� - ������� ������ ���� �� ������*/
	  set casuser.days_pbo_date_close;
	run;
	
	proc fedsql sessref=casauto; /*������� ���������*/
	create table casuser.days_pbo_close{options replace=true} as
	select distinct * from casuser.days_pbo_close;
	quit;

/*2.2 TODO: ��������� ����� T*/
	proc fedsql sessref=casauto;
		create table casuser.plm_t{options replace=true} as
		select LIFECYCLE_CD, PREDECESSOR_DIM2_ID, PREDECESSOR_PRODUCT_ID,
		SUCCESSOR_DIM2_ID, SUCCESSOR_PRODUCT_ID, SCALE_FACTOR_PCT,
		coalesce(PREDECESSOR_END_DT,cast(intnx('day',SUCCESSOR_START_DT,-1) as date)) as PREDECESSOR_END_DT, 
		SUCCESSOR_START_DT
		/* from &lmvLCTab */
		from &lmvInLibref..PRODUCT_CHAIN
		where LIFECYCLE_CD='T' 
		and coalesce(PREDECESSOR_END_DT,cast(intnx('day',SUCCESSOR_START_DT,-1) as date))<=date %tslit(&vf_fc_agg_end_dt)
		/* and successor_start_dt>=intnx('month',&vf_fc_start_dt,-3); */
		and successor_start_dt>=intnx('month',&vf_fc_start_dt,-8);
		/*������, ���������� "������" ������ 
		������ ����������� ������ 3 ��� ����� ���������� 
		������ ������� fc_agg_end_dt ��������*/
	quit;

    /*predcessor ����� ����������� �� predecessor_end_dt (�����), ��� ��������� ���� ����� �������*/
    proc fedsql sessref=casauto; 
		create table casuser.predessor_periods_t{options replace=true} as
		select PREDECESSOR_DIM2_ID as pbo_location_id,
		PREDECESSOR_PRODUCT_ID as product_id,
		min(PREDECESSOR_END_DT) as end_dt
		from casuser.plm_t group by 1,2
		;
	quit;

/*2.3 TODO: ��������� ������� D*/
	proc fedsql sessref=casauto;
		create table casuser.plm_d{options replace=true} as
		select LIFECYCLE_CD, PREDECESSOR_DIM2_ID, PREDECESSOR_PRODUCT_ID,
		SUCCESSOR_DIM2_ID, SUCCESSOR_PRODUCT_ID, SCALE_FACTOR_PCT,
		PREDECESSOR_END_DT, SUCCESSOR_START_DT
		/* from &lmvLCTab */
		from &lmvInLibref..PRODUCT_CHAIN
		where LIFECYCLE_CD='D'
		and predecessor_end_dt<=date %tslit(&vf_fc_agg_end_dt);
		/*������ ������ �� ��������
		  ������ ������� fc_agg_end_dt ��������*/
	quit;

/*2.4 TODO: insert-update ����� ������� �� ���� �� ����� � pmix_daily �� PLM
		� ����������� ����� �������*/
	%if %sysfunc(exist(casuser.npf_prediction)) eq 0 %then %do;
		proc fedsql sessref=casauto;
			create table casuser.pmix_daily_new{options replace=true} as
			select 
			t2.period_dt,
			t2.PRODUCT_ID,
			t2.channel_cd, 
			t2.PBO_LOCATION_ID, 
			t2.mon_dt,
			t2.ff
			from casuser.pmix_daily t2
			;
		quit;
	%end;
	%else %do;
		proc casutil;
			save incaslib="casuser" outcaslib="mn_short" casdata="npf_prediction" casout="npf_prediction.sashdat" replace;
		run;
		quit;
		
		proc fedsql sessref=casauto;
			create table casuser.pmix_daily_new{options replace=true} as
			select 
			coalesce(t1.SALES_DT,t2.period_dt) as period_dt,
			coalesce(t1.product_id,t2.PRODUCT_ID) as product_id,
			coalesce(t1.channel_cd,t2.channel_cd) as channel_cd, 
			coalesce(t1.pbo_location_id,t2.PBO_LOCATION_ID) as PBO_LOCATION_ID, 
			coalesce(cast(intnx('month',t1.sales_dt,0) as date),t2.mon_dt) as mon_dt,
			coalesce(t1.P_SUM_QTY,t2.ff) as ff
			from casuser.npf_prediction t1 full outer join casuser.pmix_daily t2
				on t1.SALES_DT =t2.period_dt and t1.product_id=t2.product_id and 
				t1.channel_cd=t2.channel_cd and t1.pbo_location_id=t2.pbo_location_id
			;
		quit;
	%end;
	
	 proc casutil;
			droptable casdata="pmix_daily" incaslib="mn_short" quiet;
			promote casdata="pmix_daily" incaslib="casuser" outcaslib="mn_short";
			save incaslib="mn_short" outcaslib="mn_short" casdata="pmix_daily" casout="pmix_daily.sashdat" replace;
	run;
	quit;
	
/*2.51 ���������� � �� ���������� �� ������� */
	proc fedsql sessref=casauto;
		create table casuser.AM_new{options replace=true} as
		select product_id,pbo_location_id, start_dt,end_dt
		from &lmvInLibref..ASSORT_MATRIX t1;
	quit;
    
	%if %sysfunc(exist(casuser.future_product_chain)) ne 0 %then %do;
		data casuser.AM_new(append=yes);
			set casuser.future_product_chain(rename=(period_start_dt=start_dt 
													period_end_dt=end_dt));
		run;
	%end;

/*2.52 TODO: ���������� T,D PLM � ��������� casuser.pmix_daily + ����� ������, 
		���� ������ ����������+��������� ��������*/
	/*������������ ������� �����-���-����, ������� ������ ���� � �������� - �� ��������� ��*/
	proc fedsql sessref=casauto;
		create table casuser.plm_dist{options replace=true} as
		select pbo_location_id,product_id, start_dt,end_dt
		from casuser.AM_new
		where start_dt between &vf_fc_start_dt and date %tslit(&vf_fc_agg_end_dt)
			  or &vf_fc_start_dt between start_dt and end_dt; /*����� ������ AM, �������������� � �������� ���������������*/
	quit;
	
	data casuser.days_prod_sale; /*��� ����� ����� ������ ����������� �� ���������� �� ��*/
	  set casuser.plm_dist;
	  format period_dt date9.;
	  keep PBO_LOCATION_ID PRODUCT_ID period_dt;
	  do period_dt=max(start_dt,&vf_fc_start_dt_sas) to min(&vf_fc_agg_end_dt_sas,end_dt);
	    output;
	  end;
	run;
	
	proc casutil;
			droptable casdata="plm_dist" incaslib="casuser" quiet;
	run;
	quit;

	/*������� ���������*/
	data casuser.days_prod_sale1;
		set casuser.days_prod_sale;
		by PBO_LOCATION_ID PRODUCT_ID period_dt;
		if first.period_dt then output;
	run;
	
	proc casutil;
		droptable casdata="days_prod_sale" incaslib="casuser" quiet;
	run;
	quit;
	
	proc fedsql sessref=casauto;
	  /*������� ������ ������� D */
	  create table casuser.plm_sales_mask{options replace=true} as
	  select t1.PBO_LOCATION_ID, t1.PRODUCT_ID, t1.period_dt
	  from  casuser.days_prod_sale1 t1 left join casuser.plm_d t2
	  on t1.product_id=t2.PREDECESSOR_PRODUCT_ID and t1.pbo_location_id=t2.PREDECESSOR_DIM2_ID
	  where t1.period_dt<coalesce(t2.PREDECESSOR_END_DT,cast(intnx('day',date %tslit(&vf_fc_agg_end_dt),1) as date));
	quit;
	
	proc casutil;
		droptable casdata="days_prod_sale1" incaslib="casuser" quiet;
	run;
	quit;
	
	proc fedsql sessref=casauto;
	  /*������� ������ ������� ���������� � ����������� �������� ��� */
	  create table casuser.plm_sales_mask1{options replace=true} as
		  select t1.PBO_LOCATION_ID, t1.PRODUCT_ID, t1.period_dt
		  from  casuser.plm_sales_mask t1 left join casuser.DAYS_PBO_CLOSE t3
		  on t1.pbo_location_id=t3.pbo_location_id and t1.period_dt=t3.period_dt
		  /*�������� � casuser.days_pbo_close - 
		  ����� ��� ������ �� ����� ��������,
		  ��� ��� �� ������ �������� � �� �� ����� ��� - �����*/
		  left join casuser.predessor_periods_t t4
		  on t1.pbo_location_id=t4.pbo_location_id and t1.product_id=t4.product_id
		/*�� plm_sales_mask1 ������� ��� predcessor ������� � ����� >end_dt*/
		  where t3.pbo_location_id is null and t3.period_dt is null
		  and ((t1.period_dt<=t4.end_dt and t4.end_dt is not null) or t4.end_dt=.)
		   /*���� ��� ���� � predcessor - ��������� �� <=���� ������, ���� ��� - �� ������� �� ����*/
	;
	quit;
	
	proc casutil;
		*droptable casdata="plm_sales_mask" incaslib="casuser" quiet;
		/* TEMP*/
			droptable casdata="predessor_periods_t" incaslib="mn_short" quiet;
			promote casdata="predessor_periods_t" incaslib="casuser" outcaslib="mn_short";
			save incaslib="mn_short" outcaslib="mn_short" casdata="predessor_periods_t" casout="predessor_periods_t.sashdat" replace;
	run;
	quit;
	
	proc casutil;
		/* TEMP*/
			droptable casdata="plm_sales_mask" incaslib="mn_short" quiet;
			promote casdata="plm_sales_mask" incaslib="casuser" outcaslib="mn_short";
			save incaslib="mn_short" outcaslib="mn_short" casdata="plm_sales_mask" casout="plm_sales_mask.sashdat" replace;
	run;
	quit;
/*=-==========================-*/
/* ������ ��� ���� ��������� � ������������� � ���������� ������� ��� ������ id 
   �� ����� �� ������ ������� � ������ ��������� ������������� ���������������?*/
    proc fedsql sessref=casauto; /*������ ��������� ���������, ������� predesessor
								��� id successor*/
		create table casuser.successor_fc{options replace=true} as
			select
			t1.period_DT,
			t2.SUCCESSOR_PRODUCT_ID as product_id,
			t1.CHANNEL_CD,
			t2.SUCCESSOR_DIM2_ID as pbo_location_id,
			t1.mon_dt,
			t1.FF*coalesce(t2.SCALE_FACTOR_PCT,100.)/100. as FF
			from casuser.pmix_daily_new t1 inner join casuser.plm_t t2 on
			t1.PRODUCT_ID=t2.PREDECESSOR_PRODUCT_ID and t1.PBO_LOCATION_ID=PREDECESSOR_DIM2_ID
			where t1.period_dt>=successor_start_dt;
	quit;
/*�������� ������ � pmix_daily_new, 
 �� append! ��������� � successor_fc! 
 ���� ����� ��� ������? - ��������� �� predcessor*/
	*data casuser.pmix_daily_new(append=force); 
	*  set casuser.successor_fc;
	*run;
    proc fedsql sessref=casauto;
		create table casuser.pmix_daily_new_{options replace=true} as
			select coalesce(t1.period_dt,t2.period_dt) as period_dt,
				coalesce(t1.product_id,t2.PRODUCT_ID) as product_id,
				coalesce(t1.channel_cd,t2.channel_cd) as channel_cd, 
				coalesce(t1.pbo_location_id,t2.PBO_LOCATION_ID) as PBO_LOCATION_ID, 
				coalesce(t1.mon_dt,t2.mon_dt) as mon_dt,
				coalesce(t1.ff,t2.ff) as ff
			from casuser.successor_fc t1 full outer join casuser.pmix_daily_new t2
				on t1.period_dt =t2.period_dt and t1.product_id=t2.product_id and 
				t1.channel_cd=t2.channel_cd and t1.pbo_location_id=t2.pbo_location_id
	;
	quit;

/*TODO: ���������� ������� � ������ �����, �������� � � ��������� ������� pmix*/
	proc casutil;
			droptable casdata="fc_w_plm" incaslib="casuser" quiet;
			*droptable casdata="successor_fc" incaslib="casuser" quiet;
			droptable casdata="pmix_daily_new" incaslib="casuser" quiet;
			droptable casdata="percent" incaslib="casuser" quiet;
	run;
	
	proc casutil;
			droptable casdata="successor_fc" incaslib="mn_short" quiet;
			promote casdata="successor_fc" incaslib="casuser" outcaslib="mn_short";
			save incaslib="mn_short" outcaslib="mn_short" casdata="successor_fc" casout="successor_fc.sashdat" replace;
	run;
	quit;
	
	proc fedsql sessref=casauto; /*��������� plm �� �������*/
		create table casuser.fc_w_plm{options replace=true} as 
			select t1.CHANNEL_CD,t1.PBO_LOCATION_ID,t1.PRODUCT_ID,t1.period_dt,
			t1.FF,
			coalesce(tpr.promo,0) as promo
			from casuser.pmix_daily_new_ t1 inner join casuser.plm_sales_mask1 t2 /*��� ����� ����� ������ �����������*/
			on t1.PBO_LOCATION_ID=t2.PBO_LOCATION_ID and t1.PRODUCT_ID=t2.PRODUCT_ID and t1.period_dt=t2.period_dt
			left join casuser.promo_w1 tpr 
			on tpr.channel_cd=t1.channel_cd and tpr.pbo_location_id=t1.PBO_LOCATION_ID and
				tpr.product_id=t1.PRODUCT_ID and tpr.period_dt=t1.period_dt
			;
	quit;

	/* #.#. Реконсилируем прогноз с PBO до PBO-SKU */
	proc fedsql sessref=casauto;
		/* #.#.1 Считаем распределение прогноза на уровне PBO-SKU */
		create table casuser.percent{options replace=true} as
			select 
				  wplm.*
				, case 
					when wplm.FF = 0 
					then 0 
					else wplm.FF / sum.sum_ff
				end as fcst_pct
			from 
				casuser.fc_w_plm as wplm
			inner join
				(
				select 
					  channel_cd
					, pbo_location_id
					, period_dt
					, sum(FF) as sum_ff
				from 
					casuser.fc_w_plm
				group by 
					  channel_cd
					, pbo_location_id
					, period_dt
				) as sum
					on wplm.pbo_location_id = sum.pbo_location_id 
					and wplm.period_dt = sum.period_dt
					and wplm.channel_cd = sum.channel_cd
		;
		/* #.#.2. Реконсилируем прогноз с PBO до PBO-SKU */
		create table casuser.fcst_reconciled{options replace=true} as
			select
				  pct.CHANNEL_CD
				, pct.pbo_location_id
				, pct.product_id
				, pct.period_dt
				, pct.FF as FF_before_rec
				, pct.fcst_pct
				, pct.promo
				, coalesce(vf.pbo_fcst * pct.fcst_pct, pct.FF) as FF
			from
				casuser.percent as pct
			left join 
				&pbo_table. as vf
			on         pct.pbo_location_id = vf.pbo_location_id 
				and pct.period_dt         = vf.sales_dt
				and pct.channel_cd        = vf.channel_cd
		;
	quit;
 
	
	proc casutil;
			
			*droptable casdata="plm_sales_mask1" incaslib="casuser" quiet;
			droptable casdata="fcst_reconciled" incaslib="mn_short" quiet;
			save incaslib="casuser" outcaslib="mn_short" casdata="fcst_reconciled" casout="fcst_reconciled.sashdat" replace;
			/* TEMP*/
			droptable casdata="plm_sales_mask1" incaslib="mn_short" quiet;
			promote casdata="plm_sales_mask1" incaslib="casuser" outcaslib="mn_short";
			save incaslib="mn_short" outcaslib="mn_short" casdata="plm_sales_mask1" casout="plm_sales_mask1.sashdat" replace;
	run;

/*2.6 TODO: �������� GC �� ������ �������� - �������� � �������� GC insert-update*/

/*2.7 TODO: ���������� ������� ����������+��������� �������� � ��������� GC*/
	
	proc fedsql sessref=casauto;
		create table casuser.fc_wo_plm_gc{options replace=true} as
			select coalesce(t1.period_dt,t2.sales_dt) as period_dt,
				coalesce(t1.channel_cd,t2.channel_cd) as channel_cd,
				coalesce(t1.pbo_location_id,t2.PBO_LOCATION_ID) as PBO_LOCATION_ID, 
				coalesce(t1.ff,t2.gc_fcst) as ff
			from casuser.daily_gc t1 full outer join &gc_table. t2
				on t1.period_dt =t2.sales_dt and 
				t1.channel_cd=t2.channel_cd and t1.pbo_location_id=t2.pbo_location_id
	;
	quit;
	
	proc fedsql sessref=casauto;
		create table casuser.fc_w_plm_gc{options replace=true} as 
			select t1.CHANNEL_CD,t1.PBO_LOCATION_ID,t1.period_dt,t1.FF
			from casuser.fc_wo_plm_gc t1 left join casuser.days_pbo_close t2
			on t1.PBO_LOCATION_ID=t2.PBO_LOCATION_ID and t1.period_dt=t2.period_dt 
			   and t1.CHANNEL_CD=t2.CHANNEL_CD
			where t2.PBO_LOCATION_ID is null and t2.period_dt is null
			   and t2.CHANNEL_CD  is null /*�� ������ ���� ���� � ��������*/
			;
	quit;

	proc casutil;
		*droptable casdata="plm_sales_mask" incaslib="casuser" quiet;
		/* TEMP*/
			droptable casdata="days_pbo_close" incaslib="mn_short" quiet;
			promote casdata="days_pbo_close" incaslib="casuser" outcaslib="mn_short";
			save incaslib="mn_short" outcaslib="mn_short" casdata="days_pbo_close" casout="days_pbo_close.sashdat" replace;
	run;
	quit;
	
	/*3. ���������� ��� �� �������*/
	/* Подготовка регулярных цен в разрезе SKU-ПБО-день */
	data casuser.price_reg_past;
		set MN_DICT.PRICE_REGULAR_PAST;
		retain _past 1;
		if start_dt ne . and end_dt ne . then
		do period_dt=max(start_dt,&VF_HIST_START_DT_SAS) to min(end_dt,&VF_FC_AGG_END_DT_SAS);
		output;
		end;
	run;
	data casuser.price_reg_future;
		set MN_DICT.PRICE_REGULAR_FUTURE;
		retain _past 0;
		if start_dt ne . and end_dt ne . then
		do period_dt=max(start_dt,&VF_HIST_START_DT_SAS) to min(end_dt,&VF_FC_AGG_END_DT_SAS);
		output;
		end;
	run;

	data casuser.prices_flat1;
		format period_dt date9. product_id pbo_location_id 32.;
		set casuser.price_reg_past casuser.price_reg_future;
		by product_id pbo_location_id period_dt _past;
		if first.period_dt then output;
	run;

	proc casutil;
		droptable casdata="price_reg_past" incaslib="casuser" quiet;
		droptable casdata="price_reg_future" incaslib="casuser" quiet;
	run;

	/* Подготовка промо-цен в разрезе ID_промо-SKU-ПБО-день */
	data casuser.price_promo_past;
		set MN_DICT.PRICE_promo_past;
		drop channel_cd;
		retain _past 1;
		if start_dt ne . and end_dt ne . and upcase(channel_cd)='ALL' then
		do period_dt=max(start_dt,&VF_HIST_START_DT_SAS) to min(end_dt,&VF_FC_AGG_END_DT_SAS);
		output;
		end;
	run;

	data casuser.price_promo_future;
		set MN_DICT.PRICE_promo_future;
		drop channel_cd;
		retain _past 0;
		if start_dt ne . and end_dt ne . and upcase(channel_cd)='ALL' then
		do period_dt=max(start_dt,&VF_HIST_START_DT_SAS) to min(end_dt,&VF_FC_AGG_END_DT_SAS);
		output;
		end;
	run;

	data casuser.prices_flat2;
		format period_dt date9. product_id pbo_location_id 32.;
		set casuser.price_promo_past casuser.price_promo_future;
		by product_id pbo_location_id promo_id period_dt _past;
		if first.period_dt then output;
	run;

	proc casutil;
		droptable casdata="PRICE_promo_past" incaslib="casuser" quiet;
		droptable casdata="price_promo_future" incaslib="casuser" quiet;
	run;

	/* Агрегация промо-цен до SKU-ПБО-день,
		то есть устранение разреза ID_промо */
	proc fedsql sessref=casauto;
		create table casuser.prices_flat2_nopromo{options replace=true} as
		select period_dt, product_id, pbo_location_id,
			avg(GROSS_PRICE_AMT) as A_GROSS_PRICE_AMT,
			avg(NET_PRICE_AMT) as A_NET_PRICE_AMT,
			min(GROSS_PRICE_AMT) as M_GROSS_PRICE_AMT,
			min(NET_PRICE_AMT) as M_NET_PRICE_AMT,
			count(*) as promo_ct
		from casuser.prices_flat2
		group by 1,2,3
	;
	quit;

	proc casutil;
		droptable casdata="PRICES_flat2" incaslib="casuser" quiet;
	run;

	/* Объединение промо- и регулярных цен, расчет скидок */
	data casuser.price_feat;
		merge 
			casuser.prices_flat1 (
				rename=( 
					net_price_amt = price_reg_net 			
					gross_price_amt = price_reg_gross
					)
				) 
			casuser.prices_flat2_nopromo (
				rename=( 
					M_GROSS_PRICE_AMT = price_promo_gross 	
					M_NET_PRICE_AMT = price_promo_net
					)
				)
			;
		by 
			product_id 
			pbo_location_id 
			period_dt
			;
		keep 
			product_id 
			pbo_location_id 
			period_dt 
			price_reg_gross 
			price_promo_gross
			discount_gross_rur 
			discount_gross_pct 
			price_reg_net
			price_promo_net
			discount_net_rur 
			discount_net_pct 
			promo_ct 
			price_gross
			price_net
			;
		/* GROSS-prices */
		if price_promo_gross>0 then do;
			price_gross			= price_promo_gross ;
			discount_gross_rur	= max(0, price_reg_gross - price_promo_gross);
			discount_gross_pct 	= divide(discount_gross_rur, price_reg_gross);
		end;
		else do;
			price_gross			= price_reg_gross;
			discount_gross_rur	= 0;
			discount_gross_pct	= 0;
		end;
		/* NET-prices */
		if price_promo_net>0 then do;
			price_net			= price_promo_net ;
			discount_net_rur	= max(0, price_reg_net - price_promo_net);
			discount_net_pct	= divide(discount_net_rur, price_reg_net);
		end;
		else do;
			price_net			= price_reg_net;
			discount_net_rur	= 0;
			discount_net_pct	= 0;
		end;
		promo_ct = coalesce(promo_ct,0);
	run;
	
	
	
	%if &mpPrmt. = Y %then %do;
		proc casutil;
		droptable casdata="&lmvOutTabNameGcSt." incaslib="&lmvOutLibrefGcSt." quiet;
		droptable casdata="&lmvOutTabNameUptSt." incaslib="&lmvOutLibrefUptSt." quiet;
		droptable casdata="&lmvOutTabNamePmixSt." incaslib="&lmvOutLibrefPmixSt." quiet;
		quit;
	%end;

	proc casutil;
		save incaslib="casuser" outcaslib="mn_short" casdata="price_feat" casout="price_feat.sashdat" replace;
		
		save incaslib="casuser" outcaslib="mn_short" casdata="fc_w_plm" casout="fc_w_plm.sashdat" replace;
		
		save incaslib="casuser" outcaslib="mn_short" casdata="fc_w_plm_gc" casout="fc_w_plm_gc.sashdat" replace;
		
		save incaslib="casuser" outcaslib="mn_short" casdata="fc_wo_plm_gc" casout="fc_wo_plm_gc.sashdat" replace;
	run;
	quit;
	
/*4. ������������ ������ �� ����*/
/*4.1 Units*/
	proc fedsql sessref=casauto;
	create table &lmvOutLibrefPmixSt..&lmvOutTabNamePmixSt.{options replace=true} as
		select distinct
			cast(t1.product_id as integer) as PROD /*� �� ��������*/,
			cast(t1.pbo_location_id as integer) as LOCATION /*� �� ���������*/,
			t1.period_dt as DATA /*� ���� �������� ��� ����� (����)*/,
			'RUR' as CURRENCY /*� ������, �������� �� ��������� RUR*/,
			/*'CORP' as ORG � �����������, �������� �� ��������� CORP*/
			case when promo=0 then t1.FF else 0 end
			as BASE_FCST_UNITS /*� ������� ������� (�����������, ���� � ���� ������� 
							�����-���-���� �� ���� �� ����� �����-�����, =0 �����)*/,
			case when t1.promo=1 then t1.FF else 0 end
			as PROMO_FCST_UNITS /*� ������� ����� (�����������, ���� � ���� ������� 
							�����-���-���� ���� ���� � ����� �����-�����, =0 �����)*/,
			t1.FF as FINAL_FCST_UNITS /*� ����� �������� �������� � �����*/,
			t1.FF as OVERRIDED_FCST_UNITS /*� ����� �������� �������� � ����� (��� ���������� �� ���������� ������?)*/,
			1 as OVERRIDE_TRIGGER /*� ������ ���������, �� ��������� �������� 1*/,
			case when promo=0 then t1.ff*t2.price_net else 0 end
			as BASE_FCST_SALE /*� ������� ������� � ��� (��� ��������� ���� � ����� ������������ net-����? 
						��� gross? �����������, ���� � ���� ������� �����-���-���� ��� �� ����� �����-�����)*/,
			case when promo=1 then t1.ff*t2.price_net else 0 end
			as PROMO_FCST_SALE /*� ����� ������� � ��� (�����������, ���� � ���� ������� �����-���-���� ���� ���� � ����� �����-�����)*/,
			t1.ff*t2.price_net as FINAL_FCST_SALE /*� ��������� ������� � ���*/,
			t1.ff*t2.price_net as OVERRIDED_FCST_SALE /*� ������� � ������ �������� ��� (��������� � ETL ����� ��������� ������� ���� �� ������� � ������ ����������).*/,
			t2.price_net as AVG_PRICE /*� ������� ����. ��������� � ETL ��� ��������� ������� � ���/������� � �� � ������� ���/���*/
			from casuser.fcst_reconciled t1 
			left join casuser.price_feat t2 
				on  t1.product_id		= t2.product_id 
				and t1.pbo_location_id	= t2.pbo_location_id 
				and t1.period_dt		= t2.period_dt
			where t1.channel_cd='ALL' and t1.period_dt between &VF_FC_START_DT and &VF_FC_END_SHORT_DT;
	quit;

/*4.2 GC:*/
	proc fedsql sessref=casauto;
	create table &lmvOutLibrefGcSt..&lmvOutTabNameGcSt.{options replace=true} as
		select distinct
			1 as PROD /*� �� �������� �� ������� ������ (ALL Product, �������� = 1)*/,
			cast(pbo_location_id as integer) as LOCATION /*� �� ���������*/,
			period_dt as DATA /*� ���� �������� ��� ����� (����)*/,
			'RUR' as CURRENCY /*� ������, �������� �� ��������� RUR*/,
			/*'CORP' as ORG � �����������, �������� �� ��������� CORP*/
			FF as BASE_FCST_GC /*� ������� ������� */,
			0 as PROMO_FCST_GC /*� ������� �����*/,
			FF as FINAL_FCST_GC /*� ����� �������� �������� � �����*/,
			FF as OVERRIDED_FCST_GC /*� ����� �������� �������� � ����� � ������ ����������*/,
			1 as OVERRIDE_TRIGGER /*� ������ ���������, �� ��������� �������� 1*/
			from casuser.fc_w_plm_gc
			where channel_cd='ALL' and period_dt between &VF_FC_START_DT and &VF_FC_END_SHORT_DT;
	quit;

/*4.3 UPT �� ����*/
	/*������� UPT �������������� �� �������� � �� � GC �� �������
	������� UPT(�����, ���, ����) = ������� � ��(�����, ���, ����)/������� GC(���, ����)*1000
	*/
	proc fedsql sessref=casauto;
	create table &lmvOutLibrefUptSt..&lmvOutTabNameUptSt.{options replace=true} as
		select distinct
			cast(t1.prod as integer) as PROD /*� �� �������� �� ������� ������ (ALL Product, �������� = 1) */,
			cast(t1.location as integer) as LOCATION /*� �� ���������*/,
			t1.data as DATA /*� ���� �������� ��� ����� (����)*/,
			'RUR' as CURRENCY /*� ������, �������� �� ��������� RUR*/,
			/*'CORP' as ORG � �����������, �������� �� ��������� CORP*/
		case when t2.BASE_FCST_GC is not null and abs(t2.BASE_FCST_GC)> 1e-5 
		   then t1.BASE_FCST_UNITS/t2.BASE_FCST_GC*1000 
		   else 0
		   end
		   as BASE_FCST_UPT /*� ������� �������, = ������� � ��(�����, ���, ����)/������� GC(���, ����)*1000,
						���� � ������� �����-���-���� ��� �� ����� �����-�����, =0 �����.*/,
		case when t2.BASE_FCST_GC is not null and abs(t2.BASE_FCST_GC)> 1e-5
		   then t1.PROMO_FCST_UNITS/t2.BASE_FCST_GC*1000 
		   else 0
		   end
		   as PROMO_FCST_UPT /*� ������� �����, = ������� � ��(�����, ���, ����)/������� GC(���, ����)*1000, 
						���� � ������� �����-���-���� ���� ���� ��� ����� �����-�����, =0 �����.*/,
		   1 as OVERRIDE_TRIGGER_D /*� ������ ���������, �� ��������� �������� 1*/
		from &lmvOutLibrefPmixSt..&lmvOutTabNamePmixSt. t1 left join &lmvOutLibrefGcSt..&lmvOutTabNameGcSt. t2
		  on t1.location=t2.location and t1.data=t2.data;
	quit;
	%if &mpPrmt. = Y %then %do;
		proc casutil;
		promote casdata="&lmvOutTabNamePmixSt." incaslib="&lmvOutLibrefPmixSt." outcaslib="&lmvOutLibrefPmixSt.";
		save incaslib="&lmvOutLibrefPmixSt." outcaslib="&lmvOutLibrefPmixSt." casdata="&lmvOutTabNamePmixSt." casout="&lmvOutTabNamePmixSt..sashdat" replace;
		
		promote casdata="&lmvOutTabNameGcSt." incaslib="&lmvOutLibrefGcSt." outcaslib="&lmvOutLibrefGcSt.";
		save incaslib="&lmvOutLibrefGcSt." outcaslib="&lmvOutLibrefGcSt." casdata="&lmvOutTabNameGcSt." casout="&lmvOutTabNameGcSt..sashdat" replace;
		
		promote casdata="&lmvOutTabNameUptSt." incaslib="&lmvOutLibrefUptSt." outcaslib="&lmvOutLibrefUptSt.";
		save incaslib="&lmvOutLibrefUptSt." outcaslib="&lmvOutLibrefUptSt." casdata="&lmvOutTabNameUptSt." casout="&lmvOutTabNameUptSt..sashdat" replace;
		quit;
	%end;

/*5. ��������� �� ������� GC, UPT, Pmix, �� ���� ��������� ������������� �������*/
	%if &mpPrmt. = Y %then %do;
		proc casutil;
			droptable casdata="&lmvOutTabNameGcLt." incaslib="&lmvOutLibrefGcLt." quiet;
			droptable casdata="&lmvOutTabNameUptLt." incaslib="&lmvOutLibrefUptLt." quiet;
			droptable casdata="&lmvOutTabNamePmixLt." incaslib="&lmvOutLibrefPmixLt." quiet;
			droptable casdata="&lmvOutTabNameOutfor." incaslib="&lmvOutLibrefOutfor." quiet;
			droptable casdata="&lmvOutTabNameOutforgc." incaslib="&lmvOutLibrefOutforgc." quiet;
		quit;
		
	%end;
	
/*5.1 Units*/
	proc fedsql sessref=casauto;
		create table &lmvOutLibrefPmixLt..&lmvOutTabNamePmixLt.{options replace=true} as
			select distinct
			cast(t1.product_id as integer) as PROD /*� �� ��������*/,
			cast(t1.pbo_location_id as integer) as LOCATION /*� �� ���������*/,
			cast(intnx('month',t1.period_dt,0,'b') as date) as DATA /*� ����� �������� ��� ����� � ������� (���� 1-�� ����� ������ �������� ��� �����).*/,
			'RUR' as CURRENCY /*� ������, �������� �� ��������� RUR*/,
			/*'CORP' as ORG � �����������, �������� �� ��������� CORP*/
			sum(case when promo=0 then t1.FF else 0 end) 
			   as BASE_FCST_UNITS /*� ������� �������*/,
			sum(case when promo=1 then t1.FF else 0 end)
			   as PROMO_FCST_UNITS /*� ������� �����*/,
			sum(FF) as FINAL_FCST_UNITS /*� ����� �������� �������� � �����*/,
			sum(FF) as OVERRIDED_FCST_UNITS /*� ����� �������� �������� � �����*/,
			1 as OVERRIDE_TRIGGER /*� ������ ���������, �� ��������� �������� 1*/,
			sum(case when promo=0 then t1.ff*t2.price_net else 0 end)
			   as BASE_FCST_SALE /*� ������� ������� � ���*/,
			sum(case when promo=1 then t1.ff*t2.price_net else 0 end)
			   as PROMO_FCST_SALE /*� ����� ������� � ���*/,
			sum(t1.ff*t2.price_net)
			   as FINAL_FCST_SALE /*� ��������� ������� � ���*/,
			sum(t1.ff*t2.price_net)
			   as OVERRIDED_FCST_SALE /*� ������� � ������ �������� ��� (��������� � ETL ����� ��������� ������� ���� �� ������� � ������ ����������).*/,
			case when abs(sum(t1.ff))>1e-5 then sum(t1.ff*t2.price_net)/sum(t1.ff) else 0 end
			   as AVG_PRICE /*� ������� ����. ��������� � ETL ��� ��������� ������� � ���/������� � �� � ������� ���/���*/
			from casuser.fcst_reconciled t1 
			left join casuser.price_feat t2 
				on  t1.product_id		= t2.product_id 
				and t1.pbo_location_id	= t2.pbo_location_id 
				and t1.period_dt		= t2.period_dt
			where t1.channel_cd='ALL' 
				group by 1,2,3,4;
	quit;
/*5.2 GC*/
	proc fedsql sessref=casauto;
		create table &lmvOutLibrefGcLt..&lmvOutTabNameGcLt.{options replace=true} as
			select distinct
			1 as PROD /*� �� �������� �� ������� ������ (ALL Product, �������� = 1)*/,
			cast(t1.pbo_location_id as integer) as LOCATION /*� �� ���������*/,
			cast(intnx('month',t1.period_dt,0,'b') as date) as DATA /*� ���� �������� ��� ����� (�����)*/,
			'RUR' as CURRENCY /*� ������, �������� �� ��������� RUR*/,
			/*'CORP' as ORG � �����������, �������� �� ��������� CORP*/
			sum(t1.ff) as BASE_FCST_GC /*� ������� ������� �� �����*/,
			sum(t1.ff) as OVERRIDED_FCST_GC /*� ������� ������� �� ����� (���� ������ ���������� ����������)*/,
			1 as OVERRIDE_TRIGGER /*� ������ ���������, �� ��������� �������� 1*/
			from casuser.fc_w_plm_gc t1
				where channel_cd='ALL'
				group by 1,2,3,4;
	quit;
/*5.3 UPT*/
	proc fedsql sessref=casauto;
		create table &lmvOutLibrefUptLt..&lmvOutTabNameUptLt.{options replace=true} as
			select distinct
			cast(t1.prod as integer) as PROD /*� �� ��������*/, 
			cast(t1.location as integer) as LOCATION /*� �� ���������*/,
			t1.data as DATA /*� ���� �������� ��� ����� (�����)*/,
			'RUR' as CURRENCY /*� ������, �������� �� ��������� RUR*/,
			/*'CORP' as ORG � �����������, �������� �� ��������� CORP*/
			case when t2.BASE_FCST_GC is not null and abs(t2.BASE_FCST_GC)>1e-5 
			   then t1.BASE_FCST_UNITS/t2.BASE_FCST_GC*1000 
			   else 0
			   end
			   as BASE_FCST_UPT /*� ������� �������*/,
			case when t2.BASE_FCST_GC is not null and abs(t2.BASE_FCST_GC)>1e-5 
			   then t1.PROMO_FCST_SALE/t2.BASE_FCST_GC*1000 
			   else 0
			   end
			   as PROMO_FCST_UPT /*� ����� �������*/,
			case when t2.BASE_FCST_GC is not null and abs(t2.BASE_FCST_GC)>1e-5 
			   then t1.FINAL_FCST_UNITS/t2.BASE_FCST_GC*1000 
			   else 0
			   end
			   as FINAL_FCST_UPT /*� ��������� �������*/,
			case when t2.BASE_FCST_GC is not null and abs(t2.BASE_FCST_GC)>1e-5 
			   then t1.FINAL_FCST_UNITS/t2.BASE_FCST_GC*1000 
			   else 0
			   end
			   as OVERRIDED_FCST_UPT /*� ��������� ������� (� ������ ������ ���������� ����������)*/,
			1 as OVERRIDE_TRIGGER /*� ������ ��� ���������� ���������, �� ��������� ����� 1*/
			from &lmvOutLibrefPmixLt..&lmvOutTabNamePmixLt. t1 left join &lmvOutLibrefGcLt..&lmvOutTabNameGcLt. t2
			  on t1.location=t2.location and t1.data=t2.data;
	quit;
	%if &mpPrmt. = Y %then %do;
		proc casutil;
			promote casdata="&lmvOutTabNamePmixLt." incaslib="&lmvOutLibrefPmixLt." outcaslib="&lmvOutLibrefPmixLt.";
			save incaslib="&lmvOutLibrefPmixLt." outcaslib="&lmvOutLibrefPmixLt." casdata="&lmvOutTabNamePmixLt." casout="&lmvOutTabNamePmixLt..sashdat" replace;
			promote casdata="&lmvOutTabNameGcLt." incaslib="&lmvOutLibrefGcLt." outcaslib="&lmvOutLibrefGcLt.";
			save incaslib="&lmvOutLibrefGcLt." outcaslib="&lmvOutLibrefGcLt." casdata="&lmvOutTabNameGcLt." casout="&lmvOutTabNameGcLt..sashdat" replace;
			promote casdata="&lmvOutTabNameUptLt." incaslib="&lmvOutLibrefUptLt." outcaslib="&lmvOutLibrefUptLt.";
			save incaslib="&lmvOutLibrefUptLt." outcaslib="&lmvOutLibrefUptLt." casdata="&lmvOutTabNameUptLt." casout="&lmvOutTabNameUptLt..sashdat" replace;
			/*
			promote casdata="fc_w_plm" incaslib="casuser" outcaslib="mn_short";
			save incaslib="mn_short" outcaslib="mn_short" casdata="fc_w_plm" casout="fc_w_plm.sashdat" replace;
			*/
		quit;
	%end;
%mend rtp_7_out_integration;
