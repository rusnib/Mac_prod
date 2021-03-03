%tech_redirect_log(mpMode=START, mpJobName=krevedko_sh, mpArea=Main);
	%put _ALL_;

	%tech_get_token(mpUsername=ru-nborzunov, mpOutToken=token);
	%put &=token;
	
	filename resp TEMP;
	proc http
			method="GET"
			url="https://10.252.151.9/SASJobExecution/?_program=/Public/krevedko"
			oauth_bearer = sas_services
			out=resp;
			headers
				"Authorization"="bearer &token."
				"Accept"="application/vnd.sas.job.execution.job+json";
		run;


%tech_redirect_log(mpMode=END, mpJobName=krevedko_sh, mpArea=Main);
