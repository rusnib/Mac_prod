%macro fcst_reconciliation(
							mpInputPmixTable = 
							,mpInputPboTable = 
							,mpOutputTable = 
							);

	%local 
			lmvInPmixTableNm
			lmvInPmixLibref
			lmvInPboTableNm
			lmvInPboLibref
			lmvInOutTable
			lmvInOutLibref
	;
	
	%member_names (mpTable=&mpInputPmixTable, mpLibrefNameKey=lmvInPmixLibref, mpMemberNameKey=lmvInPmixTableNm);
	%member_names (mpTable=&mpInputPboTable, mpLibrefNameKey=lmvInPboLibref, mpMemberNameKey=lmvInPboTableNm);
	%member_names (mpTable=&mpOutputTable, mpLibrefNameKey=lmvInOutLibref, mpMemberNameKey=lmvInOutTable);
	
	/* Реконсилируем прогноз с ПБО на юниты */
	proc fedsql sessref=casauto;
		/* Считаем распределение прогноза на уровне мастеркода */
		create table casuser.percent{options replace=true} as
			select
				t1.*,
				case 
					when t1.p_sum_qty = 0 
					then 0 
					else t1.p_sum_qty / t2.sum_prediction
				end as pcnt_prediction
			from
				&lmvInPmixLibref..&lmvInPmixTableNm. as t1
			inner join
				(
				select
					t1.pbo_location_id,
					t1.sales_dt,
					sum(t1.p_sum_qty) as sum_prediction
				from
					&lmvInPmixLibref..&lmvInPmixTableNm. as t1
				group by
					t1.pbo_location_id,
					t1.sales_dt
				) as t2
			on
				t1.pbo_location_id = t2.pbo_location_id and
				t1.sales_dt = t2.sales_dt
		;
		/* Реконсилируем прогноз с ПБО на юниты */
		create table casuser.fact_predict_cmp_net{options replace=true} as
			select
				t1.*,
				coalesce(t1.pcnt_prediction * t2.pbo_fcst, t1.p_sum_qty) as p_rec_sum_qty
			from
				casuser.percent as t1
			left join
				&lmvInPboLibref..&lmvInPboTableNm. as t2
			on
				t1.pbo_location_id = t2.pbo_location_id and
				t1.sales_dt = t2.sales_dt
			inner join MN_DICT.ENCODING_CHANNEL_CD c_enc
			on t1.channel_cd = c_enc.channel_cd_id 
			where
				c_enc.CHANNEL_CD = 'ALL'

		;
	quit;

	proc casutil;
		droptable incaslib="&lmvInOutLibref." casdata="&lmvInOutTable." quiet;
		promote casdata='fact_predict_cmp_net' incaslib='casuser' outcaslib="&lmvInOutLibref." casout="&lmvInOutTable.";
		save incaslib="&lmvInOutLibref." outcaslib="&lmvInOutLibref." casdata="&lmvInOutTable." casout="&lmvInOutTable..sashdat" replace; 
	run;
			
%mend fcst_reconciliation;