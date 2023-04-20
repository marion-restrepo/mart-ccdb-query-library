WITH entry2_cte AS (
	SELECT
	    patient_id,
	    encounter_id AS entry_encounter_id,
		date AS entry_date, 
	    CONCAT(patient_id, ROW_NUMBER () OVER (PARTITION BY patient_id ORDER BY date)) AS entry2_id,
	    1 AS one
	FROM mental_health_intake),
entry1_cte AS (
	SELECT
	    patient_id, 
	    entry_encounter_id,
		entry_date,
	    entry2_id::int+one AS entry1_id
	FROM entry2_cte),
active_patients AS (
	SELECT
		e1.patient_id, 
		e1.entry_encounter_id,
		e1.entry_date, 
		CASE
			WHEN mhd.discharge_date NOTNULL THEN mhd.discharge_date::date
			ELSE CURRENT_DATE 
		END AS discharge_date,
		mhd.encounter_id AS discharge_encounter_id
	FROM entry1_cte e1
	LEFT OUTER JOIN entry2_cte e2
		ON e1.entry1_id = e2.entry2_id::int
	LEFT OUTER JOIN (SELECT patient_id, discharge_date, encounter_id FROM mental_health_discharge) mhd
		ON e1.patient_id = mhd.patient_id 
		AND mhd.discharge_date > e1.entry_date 
		AND (mhd.discharge_date < e2.entry_date OR e2.entry_date IS NULL)
	ORDER BY e1.patient_id, e2.entry_date),
range_values AS (
	SELECT 
		date_trunc('day',min(ap.entry_date)) AS minval,
		date_trunc('day',max(ap.discharge_date)) AS maxval
	FROM active_patients AS ap),
day_range AS (
	SELECT 
		generate_series(minval,maxval,'1 day'::interval) as day
	FROM range_values),   
daily_admissions AS (
	SELECT 
		date_trunc('day', ap.entry_date) AS day,
		count(*) AS patients
	FROM active_patients AS ap
	GROUP BY 1),
daily_exits AS (
	SELECT
		date_trunc('day',ap.discharge_date) AS day,
		count(*) AS patients
	FROM active_patients AS ap
	GROUP BY 1),
daily_active_patients AS (
	SELECT 
		day_range.day as reporting_day,
		sum(daily_admissions.patients) over (order by day_range.day asc rows between unbounded preceding and current row) AS cumulative_admissions,
		CASE
		    WHEN sum(daily_exits.patients) over (order by day_range.day asc rows between unbounded preceding and current row) IS NULL
		    THEN 0
		    ELSE sum(daily_exits.patients) over (order by day_range.day asc rows between unbounded preceding and current row) 
		END AS cumulative_exits, 
		CASE
		    WHEN sum(daily_exits.patients) over (order by day_range.day asc rows between unbounded preceding and current row) IS NULL 
			THEN sum(daily_admissions.patients) over (order by day_range.day asc rows between unbounded preceding and current row)
		    ELSE (sum(daily_admissions.patients) over (order by day_range.day asc rows between unbounded preceding and current row)-
				sum(daily_exits.patients) over (order by day_range.day asc rows between unbounded preceding and current row)) 
		END AS active_patients,
		CASE 
			WHEN date(day_range.day)::date = (date_trunc('MONTH', day_range.day) + INTERVAL '1 MONTH - 1 day')::date THEN 1
		END AS last_day_of_month
	FROM day_range
	LEFT OUTER JOIN daily_admissions ON day_range.day = daily_admissions.day
	LEFT OUTER JOIN daily_exits ON day_range.day = daily_exits.day)
SELECT 
	dap.reporting_day,
	dap.active_patients
FROM daily_active_patients dap
WHERE dap.last_day_of_month = 1 and dap.reporting_day > date_trunc('MONTH', CURRENT_DATE) - INTERVAL '1 year'
GROUP BY dap.reporting_day, dap.active_patients