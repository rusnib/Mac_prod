%macro tech_get_token(mpUsername=sas, mpOutToken=tmp_token);
	%global lmvPasswordTmp &mpOutToken.;
	%tech_get_passw(mpOutPassword=lmvPasswordTmp);

	filename resp TEMP;
	%let lmvAPI_URL = &CUR_API_URL.;
	proc http
	  method="POST"
	  url="&lmvAPI_URL./SASLogon/oauth/token"
	  in="grant_type=password%nrstr(&)username=&mpUsername.%nrstr(&)password=&lmvPasswordTmp."
	  out=resp;
	  headers
	    "Authorization"="Basic c2FzLmVjOg=="
	    "Accept"="application/json"
	    "Content-Type"="application/x-www-form-urlencoded";
	run;
	
	%SYMDEL lmvPasswordTmp; 
	
	%put Response status: &SYS_PROCHTTP_STATUS_CODE;
	
	libname respjson JSON fileref=resp;
	data _null_;
	  set respjson.root;
	  call symputx('lmvT', access_token);
	run;

	%let &mpOutToken=&lmvT;
	
	%if not (&SYS_PROCHTTP_STATUS_CODE = 200) %then %do;
	  %put ERROR: An invalid response was received.;
	  %abort;
	%end;

%mend tech_get_token;