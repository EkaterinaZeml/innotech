/*
	dfct_phone - Список контактов клиентов физлиц с историей их изменения 

	Параметры
		&p_load_dt - дата запуска расчёта в формате '2023-05-05'
*/

drop table if exists fl;
create TABLE fl as
-- таблица физ лиц на текущую дату
select c.*, t.counterparty_type_desc
from counterparty c
join dict_counterparty_type_cd t
	on c.counterparty_type_cd = t.counterparty_type_cd
	and current_date between t.effective_from_date and t.effective_to_date --cast(&p_load_dt as date) /* параметр даты запуска расчёта, по умолчанию соответствует current_datе */
	and t.counterparty_type_desc = 'физическое лицо'
where current_date between c.effective_from_date and c.effective_to_date /* параметр даты запуска расчёта, по умолчанию соответствует current_datе */
  AND c.src_cd = 'MDMP';
 
drop table if exists all_phone;
create TABLE all_phone  AS
-- таблица всех телефонов
WITH all_phone_01 as
(
	select coalesce(cu.uniq_counterparty_rk, cc.counterparty_rk) as mdm_customer_rk
		,cc.effective_from_date
		,cc.effective_to_date
		,cc.contact_desc
		,cc.contact_type_cd
	    ,cc.contact_quality_code
		,cc.trust_system_flg
		,cc.src_cd
	from counterparty_contact cc
	left join counterparty_x_uniq_counterparty cu
		on cc.counterparty_rk = cu.counterparty_rk
		   AND cu.src_cd ='MDMP'
),
-- добавляем расчет по кол-ву дубликатов номера телефона
all_phone_02 as
(
	select (select count(1) 
				from all_phone_01 
				where contact_desc = a.contact_desc 
					and effective_to_date between a.effective_from_date and a.effective_to_date
				having count(1) > 1) cnt_dubl_phone
		, a.*
	from all_phone_01 a
),
all_phone_03 as 
-- определяем приоритеты для main флагов
(
	select
		row_number() over (partition by a.mdm_customer_rk 
							order by
									case when a.cnt_dubl_phone is null
										then 1
										else 2
									end,
									case when a.trust_system_flg = TRUE
										then 1
										else 2
									end,
									case when a.contact_quality_code like '%good%'
										then 1
										else 2
									end,
									case a.src_cd
										when 'mdmp' then 1
										when 'wayn' then 2
										when 'rtll' then 3
										when 'rtls' then 4
										when 'cftb' then 5
									end,
									case a.contact_type_cd
										when 'notificphone' 		then 1
										when 'atmphone' 			then 2
										when 'mobilepersonalphone' 	then 3
										when 'mobileworknumber' 	then 4
										when 'homephone' 			then 5 
									end,
									a.effective_from_date desc
		) main_dup_rn
		,row_number() over (partition by a.mdm_customer_rk 
							order by
									case when a.trust_system_flg = TRUE
										then 1
										else 2
									end,
									case when a.contact_quality_code like '%good%'
										then 1
										else 2
									end,
									case a.src_cd
										when 'mdmp' then 1
										when 'wayn' then 2
										when 'rtll' then 3
										when 'rtls' then 4
										when 'cftb' then 5
									end,
									case a.contact_type_cd
										when 'notificphone' 		then 1
										when 'atmphone' 			then 2
										when 'mobilepersonalphone' 	then 3
										when 'mobileworknumber' 	then 4
										when 'homephone' 			then 5 
									end,
									a.effective_from_date desc
		) main_phone_rn
		,a.mdm_customer_rk
		,a.effective_from_date as business_start_dt
		,a.effective_to_date as business_end_dt
		,a.contact_desc as phone_num
		,a.contact_type_cd as phone_type_cd
		,a.contact_quality_code
		,a.src_cd
		,contact_type_cd LIKE 'NotificPhone'  as notification_flg
		,contact_type_cd LIKE 'ATM%'  as atm_flg
		,cast(a.trust_system_flg AS TEXT) like '1' as trust_system_flg
		,a.cnt_dubl_phone is not null as duplication_flg
	from all_phone_02 a
)

-- итог фильтруем с таблицей физ лиц на текущую дату
select 
	a.mdm_customer_rk
	,a.phone_type_cd
	,a.business_start_dt
	,a.business_end_dt
	,a.phone_num
	,a.notification_flg
	,a.atm_flg
	,a.trust_system_flg
	,a.duplication_flg
	,CAST(a.main_dup_rn AS TEXT) like '1' as main_dup_flg
	,CAST(a.main_phone_rn AS TEXT) like '1' as main_phone_flg
from all_phone_03 a
inner join fl 
	on a.mdm_customer_rk = fl.counterparty_rk
order by 1, 3;

drop table if exists dfct_phone; /* заполняем финальный объект скрипта*/
create table dfct_phone as 
SELECT *
FROM all_phone;

SELECT * 
from dfct_phone