SELECT 
	date_sk,
	full_date,
	date_dt,
	full_date_desc,
	cal_day_of_month_num,
	cal_day_week,
	cal_month_key_num,
	cal_month_name_short,
	cal_month_seq,
	cal_month_name_en,
	cal_month_name_fr,
	cal_month_num_quarter,
	cal_month_num_year,
	cal_month_num_days,
	cal_month_start_day,
	cal_month_end_day,
	cal_prior_month,
	cal_next_month,
	cal_month_next_year,
	cal_month_prior_year,
	cal_quarter_key,
	cal_quarter_name,
	cal_quarter_code,
	cal_quarter_num_year,
	cal_quarter_num_days,
	cal_quarter_start,
	cal_quarter_end,
	cal_quarter_start_month,
	cal_quarter_end_month,
	cal_prior_quarter,
	cal_next_quarter,
	cal_quarter_next_year,
	cal_quarter_prior_year,
	cal_year_num,
	cal_year_name,
	cal_year_num_days,
	cal_year_start_date,
	cal_year_end_date,
	cal_year_start_month,
	cal_year_end_month,
	cal_prior_year,
	cal_next_year,
	fisc_month_key_num,
	fisc_year_seq,
	fisc_month_name,
	fisc_month_num_quarter,
	fisc_month_num_year,
	fisc_month_num_days,
	fisc_month_start_date,
	fisc_month_end_date,
	fisc_prior_month,
	fisc_next_month,
	fisc_month_next_year,
	fisc_month_prior_year,
	fisc_quarter_key,
	fisc_quarter_name,
	fisc_quarter_code,
	fisc_quarter_num_year,
	fisc_quarter_num_days,
	fisc_quarter_start,
	fisc_quarter_end,
	fisc_quarter_start_month,
	fisc_quarter_end_month,
	fisc_prior_quarter,
	fisc_next_quarter,
	fisc_quarter_next_year,
	fisc_quarter_prior_year,
	fisc_year_num,
	fisc_year_name,
	fisc_year_num_days,
	fisc_year_start_date,
	fisc_year_end_date,
	fisc_year_start_month,
	fisc_year_end_month,
	fisc_prior_year,
	fisc_next_year,
	cas_month_key_num,
	cas_year_seq,
	cas_month_name,
	cas_month_num_quarter,
	cas_month_num_year,
	cas_month_num_days,
	cas_month_start_date,
	cas_month_end_date,
	cas_prior_month,
	cas_next_month,
	cas_month_next_year,
	cas_month_prior_year,
	cas_quarter_key,
	cas_quarter_name,
	cas_quarter_code,
	cas_quarter_num_year,
	cas_quarter_num_days,
	cas_quarter_start,
	cas_quarter_end,
	cas_quarter_start_month,
	cas_quarter_end_month,
	cas_prior_quarter,
	cas_next_quarter,
	cas_quarter_next_year,
	cas_quarter_prior_year,
	cas_year_num,
	cas_year_name,
	cas_year_num_days,
	cas_year_start_date,
	cas_year_end_date,
	cas_year_start_month,
	cas_year_end_month,
	cas_prior_year,
	cas_next_year,
	fisc_month_default,
	cas_month_default,
	cal_month_default,
	fisc_year_default,
	cas_year_default,
	cal_year_default,
	fisc_month_current,
	cas_month_current,
	cal_month_current,
	fisc_year_current,
	cas_year_current,
	cal_year_current,
	date_today,
	timeline,
	timeline_month
FROM dw.dim_time