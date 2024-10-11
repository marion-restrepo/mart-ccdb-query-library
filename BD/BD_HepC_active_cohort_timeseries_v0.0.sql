-- The initial and cohort CTEs build the frame for patients entering and exiting the cohort. This frame is based on the HepC form with visit type of 'initial visit'. The query takes all initial visit dates and matches the next initial visit date (if present) or the current date. 
WITH initial AS (
	SELECT 
		patient_id, encounter_id AS initial_encounter_id, visit_location AS initial_visit_location, date AS initial_visit_date, DENSE_RANK () OVER (PARTITION BY patient_id ORDER BY date) AS initial_visit_order, LEAD (date) OVER (PARTITION BY patient_id ORDER BY date) AS next_initial_visit_date
	FROM hepatitis_c WHERE visit_type = 'Initial visit'),
cohort_frame AS (
	SELECT
		i1.patient_id, i1.initial_encounter_id, i1.initial_visit_location, i1.initial_visit_date, CASE WHEN i1.initial_visit_order > 1 THEN 'Yes' END readmission, COALESCE(i2.initial_visit_date, CURRENT_DATE) AS end_date
	FROM initial i1
	LEFT OUTER JOIN initial i2
		ON i1.patient_id = i2.patient_id AND  i1.initial_visit_order = (i2.initial_visit_order - 1)),
-- The last appointment / form / visit CTEs extract the last appointment or form reported for each patient and is used to identify the discharge date for patients.  
last_completed_appointment AS (
	SELECT patient_id, appointment_start_time, appointment_service
	FROM (
		SELECT
			patient_id,
			appointment_start_time,
			appointment_service,
			ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY appointment_start_time DESC) AS rn
		FROM patient_appointment_default
		WHERE appointment_start_time < now() AND (appointment_status = 'Completed' OR appointment_status = 'CheckedIn')) foo
	WHERE rn = 1),
last_form AS (
	SELECT initial_encounter_id, last_form_date, last_form_type 
	FROM (
		SELECT 
			cf.patient_id,
			cf.initial_encounter_id,
			nvsl.date AS last_form_date,
			nvsl.last_form_type AS last_form_type,
			ROW_NUMBER() OVER (PARTITION BY nvsl.patient_id ORDER BY nvsl.date DESC) AS rn
		FROM cohort_frame cf
		LEFT OUTER JOIN (
			SELECT 
				patient_id, COALESCE(discharge_date, date) AS date, visit_type AS last_form_type 
			FROM hepatitis_c 
			UNION 
			SELECT patient_id, date, form_field_path AS last_form_type
			FROM vitals_and_laboratory_information) nvsl
			ON cf.patient_id = nvsl.patient_id AND cf.initial_visit_date <= nvsl.date::date AND cf.end_date >= nvsl.date::date) foo
	WHERE rn = 1),
last_visit AS (
	SELECT
		cf.initial_encounter_id,
		CASE WHEN lca.appointment_start_time >= lf.last_form_date THEN lca.appointment_start_time::date WHEN lca.appointment_start_time < lf.last_form_date THEN lf.last_form_date::date WHEN lca.appointment_start_time IS NOT NULL AND lf.last_form_date IS NULL THEN lca.appointment_start_time::date WHEN lca.appointment_start_time IS NULL AND lf.last_form_date IS NOT NULL THEN lf.last_form_date::date ELSE NULL END AS last_visit_date,
		CASE WHEN lca.appointment_start_time >= lf.last_form_date THEN lca.appointment_service WHEN lca.appointment_start_time < lf.last_form_date THEN lf.last_form_type WHEN lca.appointment_start_time IS NOT NULL AND lf.last_form_date IS NULL THEN lca.appointment_service WHEN lca.appointment_start_time IS NULL AND lf.last_form_date IS NOT NULL THEN lf.last_form_type ELSE NULL END AS last_visit_type
	FROM cohort_frame cf
	LEFT OUTER JOIN last_completed_appointment lca 
		ON cf.patient_id = lca.patient_id AND cf.initial_visit_date <= lca.appointment_start_time AND cf.end_date >= lca.appointment_start_time
	LEFT OUTER JOIN last_form lf
		ON cf.initial_encounter_id = lf.initial_encounter_id),
cohort AS (
	SELECT
		cf.patient_id,
		cf.initial_encounter_id,
		cf.initial_visit_date,
		CASE WHEN lv.last_visit_type = 'Discharge visit' THEN lv.last_visit_date ELSE NULL END AS discharge_date
	FROM cohort_frame cf
	LEFT OUTER JOIN last_visit lv
		ON cf.initial_encounter_id = lv.initial_encounter_id),
-- The following CTEs create the active patient calculation by counting each day the cumulative admissions and exits from the cohort.
range_values AS (
	SELECT 
		date_trunc('day',min(c.initial_visit_date)) AS minval,
		date_trunc('day',max(CURRENT_DATE)) AS maxval
	FROM cohort c),
day_range AS (
	SELECT 
		generate_series(minval,maxval,'1 day'::interval) as day
	FROM range_values),   
daily_admissions_total AS (
	SELECT 
		date_trunc('day', c.initial_visit_date) AS day,
		count(*) AS patients
	FROM cohort c
	GROUP BY 1),
daily_exits_total AS (
	SELECT
		date_trunc('day',c.discharge_date) AS day,
		count(*) AS patients
	FROM cohort c
	GROUP BY 1),
daily_active_patients AS (
	SELECT 
		dr.day as reporting_day,
		sum(dat.patients) over (order by dr.day asc rows between unbounded preceding and current row) AS cumulative_admissions_total,
		CASE
		    WHEN sum(det.patients) over (order by dr.day asc rows between unbounded preceding and current row) IS NULL
		    THEN 0
		    ELSE sum(det.patients) over (order by dr.day asc rows between unbounded preceding and current row) 
		END AS cumulative_exits_total, 
		CASE
		    WHEN sum(det.patients) over (order by dr.day asc rows between unbounded preceding and current row) IS NULL 
			THEN sum(dat.patients) over (order by dr.day asc rows between unbounded preceding and current row)
		    ELSE (sum(dat.patients) over (order by dr.day asc rows between unbounded preceding and current row)-
				sum(det.patients) over (order by dr.day asc rows between unbounded preceding and current row)) 
		END AS active_patients_total,
		CASE 
			WHEN date(dr.day)::date = (date_trunc('MONTH', dr.day) + INTERVAL '1 MONTH - 1 day')::date THEN 1
		END AS last_day_of_month
	FROM day_range dr
	LEFT OUTER JOIN daily_admissions_total dat ON dr.day = dat.day
	LEFT OUTER JOIN daily_exits_total det ON dr.day = det.day)
-- Main query --
SELECT 
	dap.reporting_day,
	dap.active_patients_total
FROM daily_active_patients dap
WHERE dap.last_day_of_month = 1 and dap.reporting_day > date_trunc('MONTH', CURRENT_DATE) - INTERVAL '1 year'
GROUP BY dap.reporting_day, dap.active_patients_total;