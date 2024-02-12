-- The first CTE build the frame for patients entering and exiting the cohort. This frame is based on the MH intake form and the MH discharge form. The query takes all intake dates and matches discharge dates if the discharge date falls between the intake date and the next intake date (if present).
WITH intake AS (
	SELECT 
		patient_id, encounter_id AS intake_encounter_id, date::date AS intake_date, DENSE_RANK () OVER (PARTITION BY patient_id ORDER BY date) AS intake_order, LEAD (date) OVER (PARTITION BY patient_id ORDER BY date) AS next_intake_date
	FROM mental_health_intake),
cohort AS (
	SELECT
		i.patient_id, i.intake_encounter_id, i.intake_date, mhd.encounter_id AS discharge_enctounter_id, mhd.discharge_date::date
	FROM intake i
	LEFT JOIN mental_health_discharge mhd 
		ON i.patient_id = mhd.patient_id AND mhd.discharge_date >= i.intake_date AND (mhd.discharge_date < i.next_intake_date OR i.next_intake_date IS NULL)),
-- The first psy initial assessment table extracts the date from the psy initial assessment as the cohort entry date. If multiple initial assessments are completed then the first is used.
first_psy_initial_assessment AS (
SELECT 
		DISTINCT ON (c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date) c.intake_encounter_id,
		pcia.date::date
	FROM cohort c
	LEFT OUTER JOIN psy_counselors_initial_assessment pcia
		ON c.patient_id = pcia.patient_id
	WHERE pcia.date >= c.intake_date AND (pcia.date <= c.discharge_date OR c.discharge_date IS NULL)
	GROUP BY c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date, pcia.date
	ORDER BY c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date, pcia.date ASC),
-- The first clinician initial assessment table extracts the date from the first clinician initial assesment. If multiple clinician initial assessments are completed then the first is used. This table is used in case there is no psy initial assessment date provided. 
first_clinician_initial_assessment AS (
SELECT 
		DISTINCT ON (c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date) c.intake_encounter_id,
		pmia.date::date
	FROM cohort c
	LEFT OUTER JOIN psychiatrist_mhgap_initial_assessment pmia 
		ON c.patient_id = pmia.patient_id
	WHERE pmia.date >= c.intake_date AND (pmia.date <= c.discharge_date OR c.discharge_date IS NULL)
	GROUP BY c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date, pmia.date
	ORDER BY c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_date, pmia.date ASC),
-- The following sub-tables create the active patient calculation by counting each day the cumulative admissions and exits from the cohort.
active_patients AS (
	SELECT
		CASE 
			WHEN fpia.date IS NOT NULL AND fcia.date IS NULL THEN fpia.date
			WHEN fcia.date IS NOT NULL AND fpia.date IS NULL THEN fcia.date
			WHEN fpia.date IS NOT NULL AND fcia.date IS NOT NULL AND fcia.date::date <= fpia.date::date THEN fcia.date
			WHEN fpia.date IS NOT NULL AND fcia.date IS NOT NULL AND fcia.date::date > fpia.date::date THEN fpia.date
			ELSE NULL
		END	AS enrollment_date, 
		c.discharge_date
	FROM cohort c
	LEFT OUTER JOIN first_psy_initial_assessment fpia USING(intake_encounter_id)
	LEFT OUTER JOIN first_clinician_initial_assessment fcia USING(intake_encounter_id)
	WHERE fpia.date IS NOT NULL OR fcia.date IS NOT NULL),
range_values AS (
	SELECT 
		date_trunc('day',min(ap.enrollment_date)) AS minval,
		date_trunc('day',CURRENT_DATE) AS maxval
	FROM active_patients AS ap),
day_range AS (
	SELECT 
		generate_series(minval,maxval,'1 day'::interval) as day
	FROM range_values),   
daily_admissions_total AS (
	SELECT 
		date_trunc('day', ap.enrollment_date) AS day,
		count(*) AS patients
	FROM active_patients AS ap
	GROUP BY 1),
daily_exits_total AS (
	SELECT
		date_trunc('day',ap.discharge_date) AS day,
		count(*) AS patients
	FROM active_patients AS ap
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