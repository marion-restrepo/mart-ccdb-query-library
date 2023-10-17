-- The first 3 sub-tables of the query build the frame for monitoring patient entry to and exit from the waiting list and cohort. The table is built by listing all intake dates as recorded in the MH intake form. For each exit date is matched to an intake following the logic that is occurs after the entry date but not before the next entry date, in the case that a patient enters the cohort more than once.
WITH intake_cte_2 AS (
	SELECT patient_id, encounter_id AS entry_encounter_id, date::date AS intake_date, CONCAT(patient_id, ROW_NUMBER () OVER (PARTITION BY patient_id ORDER BY date)) AS entry_id_2, 1 AS one
	FROM mental_health_intake),
intake_cte_1 AS (
	SELECT patient_id, entry_encounter_id, intake_date, entry_id_2::int+one AS entry_id_1
	FROM intake_cte_2),
entry_exit_cte AS (
	SELECT ic1.patient_id, ic1.entry_encounter_id, ic1.intake_date, mhd.discharge_date::date, mhd.encounter_id AS discharge_encounter_id
	FROM intake_cte_1 ic1
	LEFT OUTER JOIN intake_cte_2 ic2
		ON ic1.entry_id_1::int = ic2.entry_id_2::int
	LEFT OUTER JOIN (SELECT patient_id, discharge_date, encounter_id FROM mental_health_discharge) mhd
		ON ic1.patient_id = mhd.patient_id AND mhd.discharge_date >= ic1.intake_date AND (mhd.discharge_date < ic2.intake_date OR ic2.intake_date IS NULL)),
-- The first psy initial assessment table extracts the date from the psy initial assessment as the cohort entry date. If multiple initial assessments are completed then the first is used.
first_psy_initial_assessment AS (
SELECT 
		DISTINCT ON (eec.patient_id, eec.entry_encounter_id, eec.intake_date, eec.discharge_date) eec.entry_encounter_id,
		pcia.date::date
	FROM entry_exit_cte eec
	LEFT OUTER JOIN psy_counselors_initial_assessment pcia
		ON eec.patient_id = pcia.patient_id
	WHERE pcia.date >= eec.intake_date AND (pcia.date <= eec.discharge_date OR eec.discharge_date IS NULL)
	GROUP BY eec.patient_id, eec.entry_encounter_id, eec.intake_date, eec.discharge_date, pcia.date
	ORDER BY eec.patient_id, eec.entry_encounter_id, eec.intake_date, eec.discharge_date, pcia.date ASC),
-- The first clinician initial assessment table extracts the date from the first clinician initial assesment. If multiple clinician initial assessments are completed then the first is used. This table is used in case there is no psy initial assessment date provided. 
first_clinician_initial_assessment AS (
SELECT 
		DISTINCT ON (eec.patient_id, eec.entry_encounter_id, eec.intake_date, eec.discharge_date) eec.entry_encounter_id,
		pmia.date::date
	FROM entry_exit_cte eec
	LEFT OUTER JOIN psychiatrist_mhgap_initial_assessment pmia 
		ON eec.patient_id = pmia.patient_id
	WHERE pmia.date >= eec.intake_date AND (pmia.date <= eec.discharge_date OR eec.discharge_date IS NULL)
	GROUP BY eec.patient_id, eec.entry_encounter_id, eec.intake_date, eec.discharge_date, pmia.date
	ORDER BY eec.patient_id, eec.entry_encounter_id, eec.intake_date, eec.discharge_date, pmia.date ASC),
-- The following sub-tables create the active patient calculation by counting each day the cumulative admissions and exits from the cohort.
active_patients AS (
	SELECT
		CASE 
			WHEN fpia.date IS NOT NULL THEN fpia.date
			WHEN fpia.date IS NULL THEN fcia.date
			ELSE NULL
		END	AS entry_date, 
		eec.discharge_date
	FROM entry_exit_cte eec
	LEFT OUTER JOIN first_psy_initial_assessment fpia USING(entry_encounter_id)
	LEFT OUTER JOIN first_clinician_initial_assessment fcia USING(entry_encounter_id)
	WHERE fpia.date IS NOT NULL OR fcia.date IS NOT NULL),
range_values AS (
	SELECT 
		date_trunc('day',min(ap.entry_date)) AS minval,
		date_trunc('day',max(ap.entry_date)) AS maxval
	FROM active_patients AS ap),
day_range AS (
	SELECT 
		generate_series(minval,maxval,'1 day'::interval) as day
	FROM range_values),   
daily_admissions_total AS (
	SELECT 
		date_trunc('day', ap.entry_date) AS day,
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