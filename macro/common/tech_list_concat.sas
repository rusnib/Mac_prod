%macro tech_list_concat(mpVarBase=, mpVarAdd=, mpOutputVar=);
    %global &mpOutputVar.;

    %let lmvVarBase = %sysfunc(lowcase(&mpVarBase.));
    %let lmvVarAdd = %sysfunc(lowcase(&mpVarAdd.));
    %let lmvVarAddLength = %sysfunc(countw(&lmvVarAdd., %str( )));

    %do i=1 %to &lmvVarAddLength.;
        %let lmvWord = %scan(&lmvVarAdd., &i., %str( ));
        %if %sysfunc(find(&lmvVarBase., &lmvWord.)) eq 0 %then %do;
            %let lmvVarBase = %sysfunc(catx(%str( ),&lmvVarBase.,&lmvWord.));
        %end;
    %end;
	%let &mpOutputVar. = &lmvVarBase.;
    
%mend tech_list_concat;