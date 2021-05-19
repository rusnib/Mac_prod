/*
	Просто тестирование результатов.
	В дальнейшем этот скрипт можно будет удалить.
*/

proc sql;
	select
		nmiss(np_gift_price_amt) as cnt,
		count(1) as all_rows
	from
		nac.na_train
	;
quit;

proc sql;
	select
		count(1) as number_of_rows
	from
		nac.na_train
	where
		PROMO_LIFETIME < 0
	;
quit;


proc surveyselect data=nac.na_train method=srs n=10000
                  out=work.na_train_sample;
run;

proc sgplot data=work.na_train_sample;
	density np_gift_price_amt / type=kernel;
	
run;

%macro count_unique(var);
	proc sql;
		select
			&var.,
			count(1) as cnt
		from
			nac.na_train
		group by
			&var.
		;
	quit;
%mend;


%count_unique(Undefined)