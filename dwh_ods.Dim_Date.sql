CREATE TABLE dwh_ods.Dim_Date (
	date_key INT PRIMARY KEY,   
	date_actual DATE NOT NULL,   
	year INT NOT NULL,   		
	quarter INT NOT NULL,   	
	month INT NOT NULL,   		
	week INT NOT NULL,   		
	day_of_month INT NOT NULL,     
	day_of_week INT NOT NULL,   
	is_weekday BOOLEAN NOT NULL,   
	is_holiday BOOLEAN NOT NULL,
	fiscal_year INT NOT NULL
);

insert into dwh_ods.Dim_Date
select
	(extract('year' from dt)*10000 + extract('month' from dt)*100 + extract('day' from dt))::int4 as date_key
	,dt::date as date_actual
	,extract('year' from dt)::int2 as "year"
	,extract('quarter' from dt)::int2 as "quarter"
	,extract('month' from dt)::int2 as "month"
	,extract('week' from dt)::int2 as "week"
	,extract('day' from dt)::int2 as day_of_month
	,extract('isodow' from dt)::int2 as day_of_week
	,case when extract('isodow' from dt) in (6,7) then false else true end is_weekday
	,case when extract('isodow' from dt) in (6,7) then true else false end is_holiday
	,case when dt::date between concat(extract('year' from dt)-1, '04', '01')::date and concat(extract('year' from dt), '03', '31')::date 
		then extract('year' from dt) 
		else extract('year' from dt) +1 
	end fiscal_year
from generate_series(date '2000-01-01', date '2100-12-31', interval '1 day') as t(dt)
order by dt;

