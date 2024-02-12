-- The first CTE build the frame for patients entering and exiting the cohort. This frame is based on the MH intake form and the MH discharge form. The query takes all intake dates and matches discharge dates if the discharge date falls between the intake date and the next intake date (if present).
WITH intake AS (
	SELECT 
		patient_id, encounter_id AS intake_encounter_id, date AS intake_date, DENSE_RANK () OVER (PARTITION BY patient_id ORDER BY date) AS intake_order, LEAD (date) OVER (PARTITION BY patient_id ORDER BY date) AS next_intake_date
	FROM mental_health_intake),
cohort AS (
	SELECT
		i.patient_id, i.intake_encounter_id, i.intake_date, CASE WHEN i.intake_order > 1 THEN 'Yes' END readmission, mhd.encounter_id AS discharge_encounter_id, mhd.discharge_date
	FROM intake i
	LEFT JOIN mental_health_discharge mhd 
		ON i.patient_id = mhd.patient_id AND mhd.discharge_date >= i.intake_date AND (mhd.discharge_date < i.next_intake_date OR i.next_intake_date IS NULL)),
-- The first psy initial assessment CTE extracts the date from the first psy initial assessment. If multiple initial assessments are completed per cohort enrollment then the first is used.
first_psy_initial_assessment AS (
	SELECT DISTINCT ON (c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date) c.intake_encounter_id, pcia.date::date
	FROM cohort c
	LEFT OUTER JOIN psy_counselors_initial_assessment pcia
		ON c.patient_id = pcia.patient_id
	WHERE pcia.date >= c.intake_date AND (pcia.date <= c.discharge_date OR c.discharge_date IS NULL)
	GROUP BY c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date, pcia.date
	ORDER BY c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date, pcia.date ASC),
-- The first clinician initial assessment CTE extracts the date from the first clinician initial assesment. If multiple initial assessments are completed per cohort enrollment then the first is used.
first_clinician_initial_assessment AS (
	SELECT DISTINCT ON (c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date) c.intake_encounter_id, pmia.date::date
	FROM cohort c
	LEFT OUTER JOIN psychiatrist_mhgap_initial_assessment pmia 
		ON c.patient_id = pmia.patient_id
	WHERE pmia.date >= c.intake_date AND (pmia.date <= c.discharge_date OR c.discharge_date IS NULL)
	GROUP BY c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date, pmia.date
	ORDER BY c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date, pmia.date ASC),
-- The visit location CTE finds the last visit location reported across all clinical consultaiton/session forms.
last_visit_location AS (	
	SELECT 
		DISTINCT ON (c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date) c.intake_encounter_id,
		vl.visit_location AS last_visit_location
	FROM cohort c
	LEFT OUTER JOIN (
		SELECT pcia.date::date, pcia.patient_id, pcia.visit_location FROM psy_counselors_initial_assessment pcia WHERE pcia.visit_location IS NOT NULL 
		UNION 
		SELECT pmia.date::date, pmia.patient_id, pmia.visit_location FROM psychiatrist_mhgap_initial_assessment pmia WHERE pmia.visit_location IS NOT NULL 
		UNION
		SELECT pcfu.date::date, pcfu.patient_id, pcfu.visit_location FROM psy_counselors_follow_up pcfu WHERE pcfu.visit_location IS NOT NULL 
		UNION
		SELECT pmfu.date::date, pmfu.patient_id, pmfu.visit_location FROM psychiatrist_mhgap_follow_up pmfu WHERE pmfu.visit_location IS NOT NULL
		UNION
		SELECT mhd.discharge_date AS date, mhd.patient_id, mhd.visit_location FROM mental_health_discharge mhd WHERE mhd.visit_location IS NOT NULL) vl
		ON c.patient_id = vl.patient_id
	WHERE vl.date >= c.intake_date AND (vl.date <= c.discharge_date OR c.discharge_date IS NULL)
	GROUP BY c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date, vl.date, vl.visit_location
	ORDER BY c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date, vl.date DESC),
-- The following CTEs create the active patient calculation by counting each day the cumulative admissions and exits from the cohort.
active_patients AS (
	SELECT
		CASE 
			WHEN fpia.date IS NOT NULL AND fcia.date IS NULL THEN fpia.date
			WHEN fcia.date IS NOT NULL AND fpia.date IS NULL THEN fcia.date
			WHEN fpia.date IS NOT NULL AND fcia.date IS NOT NULL AND fcia.date::date <= fpia.date::date THEN fcia.date
			WHEN fpia.date IS NOT NULL AND fcia.date IS NOT NULL AND fcia.date::date > fpia.date::date THEN fpia.date
			ELSE NULL
		END	AS enrollment_date, 
		c.discharge_date,
		lvl.last_visit_location
	FROM cohort c
	LEFT OUTER JOIN first_psy_initial_assessment fpia USING(intake_encounter_id)
	LEFT OUTER JOIN first_clinician_initial_assessment fcia USING(intake_encounter_id)
	LEFT OUTER JOIN last_visit_location lvl USING(intake_encounter_id)
	WHERE fpia.date IS NOT NULL OR fcia.date IS NOT NULL),
range_values AS (
	SELECT 
		date_trunc('day',min(ap.enrollment_date)) AS minval,
		date_trunc('day',max(CURRENT_DATE)) AS maxval
	FROM active_patients AS ap),
day_range AS (
	SELECT 
		generate_series(minval,maxval,'1 day'::interval) as day
	FROM range_values),   
daily_admissions_total AS (
	SELECT 
		date_trunc('day', ap.enrollment_date) AS day,
		count(*) AS patients
	FROM active_patients ap
	GROUP BY 1),
daily_admissions_opd2 AS (
	SELECT 
		date_trunc('day', ap.enrollment_date) AS day,
		count(*) AS patients
	FROM active_patients ap
	WHERE ap.last_visit_location = 'OPD2'
	GROUP BY 1),
daily_admissions_opd3 AS (
	SELECT 
		date_trunc('day', ap.enrollment_date) AS day,
		count(*) AS patients
	FROM active_patients ap
	WHERE ap.last_visit_location = 'OPD3'
	GROUP BY 1),
daily_admissions_hoh AS (
	SELECT 
		date_trunc('day', ap.enrollment_date) AS day,
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