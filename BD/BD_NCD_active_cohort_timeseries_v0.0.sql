-- The first CTE build the frame for patients entering and exiting the cohort. This frame is based on NCD forms with visit types of 'initial visit' and 'discharge visit'. The query takes all initial visit dates and matches discharge visit dates if the discharge visit date falls between the initial visit date and the next initial visit date (if present).
WITH initial AS (
	SELECT 
		patient_id, encounter_id AS initial_encounter_id, date AS initial_visit_date, DENSE_RANK () OVER (PARTITION BY patient_id ORDER BY date) AS initial_visit_order, LEAD (date) OVER (PARTITION BY patient_id ORDER BY date) AS next_initial_visit_date
	FROM ncd WHERE visit_type = 'Initial visit'),
cohort AS (
	SELECT
		i.patient_id, i.initial_encounter_id, i.initial_visit_date, d.date AS discharge_date
	FROM initial i
	LEFT JOIN (SELECT patient_id, date FROM ncd WHERE visit_type = 'Discharge visit') d 
		ON i.patient_id = d.patient_id AND d.date >= i.initial_visit_date AND (d.date < i.next_initial_visit_date OR i.next_initial_visit_date IS NULL)),
-- The last visit location CTE finds the last visit location reported in NCD forms.
last_visit_location AS (	
	SELECT 
		DISTINCT ON (c.patient_id, c.initial_encounter_id, c.initial_visit_date, c.discharge_date) c.initial_encounter_id,
		ncd.visit_location AS last_visit_location
	FROM cohort c
	LEFT OUTER JOIN ncd
		ON c.patient_id = ncd.patient_id AND c.initial_visit_date <= ncd.date::date AND CASE WHEN c.discharge_date IS NOT NULL THEN c.discharge_date ELSE current_date END >= ncd.date::date
	WHERE ncd.visit_location IS NOT NULL
	GROUP BY c.patient_id, c.initial_encounter_id, c.initial_visit_date, c.discharge_date, ncd.date, ncd.visit_location
	ORDER BY c.patient_id, c.initial_encounter_id, c.initial_visit_date, c.discharge_date, ncd.date DESC),
-- The following CTEs create the active patient calculation by counting each day the cumulative admissions and exits from the cohort.
active_patients AS (
	SELECT
		c.initial_visit_date, 
		c.discharge_date,
		lvl.last_visit_location
	FROM cohort c
	LEFT OUTER JOIN last_visit_location lvl
		ON c.initial_encounter_id = lvl.initial_encounter_id),
range_values AS (
	SELECT 
		date_trunc('day',min(ap.initial_visit_date)) AS minval,
		date_trunc('day',max(CURRENT_DATE)) AS maxval
	FROM active_patients ap),
day_range AS (
	SELECT 
		generate_series(minval,maxval,'1 day'::interval) as day
	FROM range_values),   
daily_admissions_total AS (
	SELECT 
		date_trunc('day', ap.initial_visit_date) AS day,
		count(*) AS patients
	FROM active_patients ap
	GROUP BY 1),
daily_admissions_opd2 AS (
	SELECT 
		date_trunc('day', ap.initial_visit_date) AS day,
		count(*) AS patients
	FROM active_patients ap
	WHERE ap.last_visit_location = 'OPD2'
	GROUP BY 1),
daily_admissions_opd3 AS (
	SELECT 
		date_trunc('day', ap.initial_visit_date) AS day,
		count(*) AS patients
	FROM active_patients ap
	WHERE ap.last_visit_location = 'OPD3'
	GROUP BY 1),
daily_admissions_hoh AS (
	SELECT 
		date_trunc('day', ap.initial_visit_date) AS day,
		count(*) AS patients
	FROM active_patients ap
	WHERE ap.last_visit_location = 'HoH'
	GROUP BY 1),
daily_exits_total AS (
	SELECT
		date_trunc('day',ap.discharge_date) AS day,
		count(*) AS patients
	FROM active_patients ap
	GROUP BY 1),
daily_exits_opd2 AS (
	SELECT
		date_trunc('day',ap.discharge_date) AS day,
		count(*) AS patients
	FROM active_patients ap
	WHERE ap.last_visit_location = 'OPD2'
	GROUP BY 1),
daily_exits_opd3 AS (
	SELECT
		date_trunc('day',ap.discharge_date) AS day,
		count(*) AS patients
	FROM active_patients ap
	WHERE ap.last_visit_location = 'OPD3'
	GROUP BY 1),
daily_exits_hoh AS (
	SELECT
		date_trunc('day',ap.discharge_date) AS day,
		count(*) AS patients
	FROM active_patients ap
	WHERE ap.last_visit_location = 'HoH'
	GROUP BY 1),
daily_active_patients AS (
	SELECT 
		dr.day as reporting_day,
		SUM(dat.patients) over (order by dr.day asc rows between unbounded preceding and current row) AS cumulative_admissions_total,
		CASE
		    WHEN SUM(det.patients) over (order by dr.day asc rows between unbounded preceding and current row) IS NULL
		    THEN 0
		    ELSE SUM(det.patients) over (order by dr.day asc rows between unbounded preceding and current row) 
		END AS cumulative_exits_total, 
		CASE
		    WHEN SUM(de2.patients) over (order by dr.day asc rows between unbounded preceding and current row) IS NULL
		    THEN 0
		    ELSE SUM(de2.patients) over (order by dr.day asc rows between unbounded preceding and current row) 
		END AS cumulative_exits_opd2, 
		CASE
		    WHEN SUM(de3.patients) over (order by dr.day asc rows between unbounded preceding and current row) IS NULL
		    THEN 0
		    ELSE SUM(de3.patients) over (order by dr.day asc rows between unbounded preceding and current row) 
		END AS cumulative_exits_opd3, 
		CASE
		    WHEN SUM(deh.patients) over (order by dr.day asc rows between unbounded preceding and current row) IS NULL
		    THEN 0
		    ELSE SUM(deh.patients) over (order by dr.day asc rows between unbounded preceding and current row) 
		END AS cumulative_exits_hoh, 
		CASE
		    WHEN SUM(det.patients) over (order by dr.day asc rows between unbounded preceding and current row) IS NULL 
			THEN SUM(dat.patients) over (order by dr.day asc rows between unbounded preceding and current row)
		    ELSE (SUM(dat.patients) over (order by dr.day asc rows between unbounded preceding and current row)-
				SUM(det.patients) over (order by dr.day asc rows between unbounded preceding and current row)) 
		END AS active_patients_total,
		CASE
		    WHEN SUM(de2.patients) over (order by dr.day asc rows between unbounded preceding and current row) IS NULL 
			THEN SUM(da2.patients) over (order by dr.day asc rows between unbounded preceding and current row)
		    ELSE (SUM(da2.patients) over (order by dr.day asc rows between unbounded preceding and current row)-
				SUM(de2.patients) over (order by dr.day asc rows between unbounded preceding and current row)) 
		END AS active_patients_opd2,
		CASE
		    WHEN SUM(de3.patients) over (order by dr.day asc rows between unbounded preceding and current row) IS NULL 
			THEN SUM(da3.patients) over (order by dr.day asc rows between unbounded preceding and current row)
		    ELSE (SUM(da3.patients) over (order by dr.day asc rows between unbounded preceding and current row)-
				SUM(de3.patients) over (order by dr.day asc rows between unbounded preceding and current row)) 
		END AS active_patients_opd3,
		CASE
		    WHEN SUM(deh.patients) over (order by dr.day asc rows between unbounded preceding and current row) IS NULL 
			THEN SUM(dah.patients) over (order by dr.day asc rows between unbounded preceding and current row)
		    ELSE (SUM(dah.patients) over (order by dr.day asc rows between unbounded preceding and current row)-
				SUM(deh.patients) over (order by dr.day asc rows between unbounded preceding and current row)) 
		END AS active_patients_hoh,
		CASE 
			WHEN date(dr.day)::date = (date_trunc('MONTH', dr.day) + INTERVAL '1 MONTH - 1 day')::date THEN 1
		END AS last_day_of_month
	FROM day_range dr
	LEFT OUTER JOIN daily_admissions_total dat ON dr.day = dat.day
	LEFT OUTER JOIN daily_admissions_opd2 da2 ON dr.day = da2.day
	LEFT OUTER JOIN daily_admissions_opd3 da3 ON dr.day = da3.day
	LEFT OUTER JOIN daily_admissions_hoh dah ON dr.day = dah.day
	LEFT OUTER JOIN daily_exits_total det ON dr.day = det.day
	LEFT OUTER JOIN daily_exits_opd2 de2 ON dr.day = de2.day
	LEFT OUTER JOIN daily_exits_opd3 de3 ON dr.day = de3.day
	LEFT OUTER JOIN daily_exits_hoh deh ON dr.day = deh.day)
-- Main query --
SELECT 
	dap.reporting_day,
	dap.active_patients_total,
	dap.active_patients_opd2,
	dap.active_patients_opd3,
	dap.active_patients_hoh
FROM daily_active_patients dap
WHERE dap.last_day_of_month = 1 and dap.reporting_day > date_trunc('MONTH', CURRENT_DATE) - INTERVAL '1 year'
GROUP BY dap.reporting_day, dap.active_patients_total, dap.active_patients_opd2, dap.active_patients_opd3, dap.active_patients_hoh;