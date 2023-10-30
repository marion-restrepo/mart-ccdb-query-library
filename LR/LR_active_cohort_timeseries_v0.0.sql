-- The first CTEs build the frame for patients entering and exiting the cohort. This frame is based on the MH intake form and the MH discharge form. The query takes all intake dates and matches discharge dates if the discharge date falls between the intake date and the next intake date (if present).
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
-- The NCD diagnosis CTEs pivot NCD diagnosis data horizontally from the NCD form. Only the last diagnoses reported per cohort enrollment are present. 
ncd_diagnosis_pivot AS (
	SELECT 
		DISTINCT ON (n.encounter_id, n.patient_id, n.date::date) n.encounter_id, 
		n.patient_id, 
		n.date::date,
		MAX (CASE WHEN d.diagnosis = 'Focal epilepsy' THEN 1 ELSE NULL END) AS focal_epilepsy,
		MAX (CASE WHEN d.diagnosis = 'Generalised epilepsy' THEN 1 ELSE NULL END) AS generalised_epilepsy,
		MAX (CASE WHEN d.diagnosis = 'Unclassified epilepsy' THEN 1 ELSE NULL END) AS unclassified_epilepsy,
		MAX (CASE WHEN d.diagnosis = 'Other' THEN 1 ELSE NULL END) AS other_ncd
	FROM ncd n
	LEFT OUTER JOIN diagnosis d 
		ON d.encounter_id = n.encounter_id AND d.diagnosis IS NOT NULL 
	WHERE d.diagnosis IS NOT NULL 
	GROUP BY n.encounter_id, n.patient_id, n.date::date),
last_ncd_diagnosis AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.intake_encounter_id) c.patient_id,
		c.intake_encounter_id,
		c.intake_date, 
		c.discharge_encounter_id,
		c.discharge_date, 
		ndp.date::date,
		ndp.focal_epilepsy,
		ndp.generalised_epilepsy,
		ndp.unclassified_epilepsy,
		ndp.other_ncd
	FROM cohort c
	LEFT OUTER JOIN ncd_diagnosis_pivot ndp 
		ON c.patient_id = ndp.patient_id AND c.intake_date <= ndp.date AND CASE WHEN c.discharge_date IS NOT NULL THEN c.discharge_date ELSE current_date END >= ndp.date
	WHERE ndp.date IS NOT NULL
	GROUP BY c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_encounter_id, c.discharge_date, ndp.date, ndp.focal_epilepsy, ndp.generalised_epilepsy, ndp.unclassified_epilepsy, ndp.other_ncd
	ORDER BY c.patient_id, c.intake_encounter_id, c.intake_date, ndp.date DESC),
-- The Mental Health diagnosis CTE pivots mental health diagnosis data horizontally from the Psychiatrist mhGap initial and follow-up forms. Only the last diagnoses reported per cohort enrollment are present. 
last_mh_main_dx AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.intake_encounter_id) c.patient_id,
		c.intake_encounter_id,
		c.intake_date, 
		c.discharge_encounter_id,
		c.discharge_date,
		mmhd.date,
		mmhd.main_diagnosis AS diagnosis
	FROM cohort c
	LEFT OUTER JOIN (
		SELECT patient_id, date::date, main_diagnosis FROM psychiatrist_mhgap_initial_assessment
		UNION
		SELECT patient_id, date::date, main_diagnosis FROM psychiatrist_mhgap_follow_up) mmhd
		ON c.patient_id = mmhd.patient_id AND c.intake_date <= mmhd.date::date AND CASE WHEN c.discharge_date IS NOT NULL THEN c.discharge_date ELSE current_date END >= mmhd.date::date
	WHERE mmhd.main_diagnosis IS NOT NULL 
	GROUP BY c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_encounter_id, c.discharge_date, mmhd.date::date,	mmhd.main_diagnosis 
	ORDER BY c.patient_id, c.intake_encounter_id, c.intake_date, mmhd.date::date DESC),
last_mh_sec_dx AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.intake_encounter_id) c.patient_id,
		c.intake_encounter_id,
		c.intake_date, 
		c.discharge_encounter_id,
		c.discharge_date,
		mmhd.date,
		mmhd.secondary_diagnosis AS diagnosis
	FROM cohort c
	LEFT OUTER JOIN (
		SELECT patient_id, date::date, secondary_diagnosis FROM psychiatrist_mhgap_initial_assessment
		UNION
		SELECT patient_id, date::date, secondary_diagnosis FROM psychiatrist_mhgap_follow_up) mmhd
		ON c.patient_id = mmhd.patient_id AND c.intake_date <= mmhd.date::date AND CASE WHEN c.discharge_date IS NOT NULL THEN c.discharge_date ELSE current_date END >= mmhd.date::date
	WHERE mmhd.secondary_diagnosis IS NOT NULL 
	GROUP BY c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_encounter_id, c.discharge_date, mmhd.date::date, mmhd.secondary_diagnosis 
	ORDER BY c.patient_id, c.intake_encounter_id, c.intake_date, mmhd.date::date DESC),
last_mh_diagnosis AS (
	SELECT 
		DISTINCT ON (mhdu.patient_id, intake_encounter_id) mhdu.patient_id, 
		intake_encounter_id,
		MAX (CASE WHEN mhdu.diagnosis = 'Acute and transient psychotic disorder' THEN 1 ELSE NULL END) AS acute_transient_psychotic_disorder,	
		MAX (CASE WHEN mhdu.diagnosis = 'Acute stress reaction' THEN 1 ELSE NULL END) AS acute_stress_reaction,	
		MAX (CASE WHEN mhdu.diagnosis = 'Adjustment disorders' THEN 1 ELSE NULL END) AS adjustment_disorders,	
		MAX (CASE WHEN mhdu.diagnosis = 'Anxiety disorder' THEN 1 ELSE NULL END) AS anxiety_disorder,
		MAX (CASE WHEN mhdu.diagnosis = 'Bipolar disorder' THEN 1 ELSE NULL END) AS bipolar_disorder,	
	 	MAX (CASE WHEN mhdu.diagnosis = 'Childhood emotional disorder' THEN 1 ELSE NULL END) AS childhood_emotional_disorder,
		MAX (CASE WHEN mhdu.diagnosis = 'Conduct disorders' THEN 1 ELSE NULL END) AS conduct_disorders,
		MAX (CASE WHEN mhdu.diagnosis = 'Delirium' THEN 1 ELSE NULL END) AS delirium,	
		MAX (CASE WHEN mhdu.diagnosis = 'Dementia' THEN 1 ELSE NULL END) AS dementia,	
		MAX (CASE WHEN mhdu.diagnosis = 'Dissociative and conversion disorder' THEN 1 ELSE NULL END) AS dissociative_conversion_disorder,
		MAX (CASE WHEN mhdu.diagnosis = 'Dissociative convulsions' THEN 1 ELSE NULL END) AS dissociative_convulsions,
		MAX (CASE WHEN mhdu.diagnosis = 'Hyperkinetic disorder' THEN 1 ELSE NULL END) AS hyperkinetic_disorder,
		MAX (CASE WHEN mhdu.diagnosis = 'Intellectual disability' THEN 1 ELSE NULL END) AS intellectual_disability,
		MAX (CASE WHEN mhdu.diagnosis = 'Mental or behavioural disorders due to multiple drug use or other psychoactive substances' THEN 1 ELSE NULL END) AS disorders_due_drug_psychoactive_substances,
		MAX (CASE WHEN mhdu.diagnosis = 'Mental or behavioural disorders due to use of alcohol' THEN 1 ELSE NULL END) AS disorders_due_alcohol,
		MAX (CASE WHEN mhdu.diagnosis = 'Mild depressive episode' THEN 1 ELSE NULL END) AS mild_depressive_episode,
		MAX (CASE WHEN mhdu.diagnosis = 'Moderate depressive episode' THEN 1 ELSE NULL END) AS moderate_depressive_episode,
		MAX (CASE WHEN mhdu.diagnosis = 'Nonorganic enuresis' THEN 1 ELSE NULL END) AS nonorganic_enuresis,
		MAX (CASE WHEN mhdu.diagnosis = 'Obsessive-compulsive disorder' THEN 1 ELSE NULL END) AS obsessive_compulsive_disorder,
		MAX (CASE WHEN mhdu.diagnosis = 'Panic disorder' THEN 1 ELSE NULL END) AS panic_disorder,
		MAX (CASE WHEN mhdu.diagnosis = 'Pervasive developmental disorder' THEN 1 ELSE NULL END) AS pervasive_developmental_disorder,
		MAX (CASE WHEN mhdu.diagnosis = 'Post-partum depression' THEN 1 ELSE NULL END) AS postpartum_depression,
		MAX (CASE WHEN mhdu.diagnosis = 'Post-partum psychosis' THEN 1 ELSE NULL END) AS postpartum_psychosis,
		MAX (CASE WHEN mhdu.diagnosis = 'Post Traumatic Stress Disorder' THEN 1 ELSE NULL END) AS ptsd,
		MAX (CASE WHEN mhdu.diagnosis = 'Schizophrenia' THEN 1 ELSE NULL END) AS schizophrenia,
		MAX (CASE WHEN mhdu.diagnosis = 'Severe depressive episode with psychotic symptoms' THEN 1 ELSE NULL END) AS severe_depressive_episode_with_psychotic_symptoms,
		MAX (CASE WHEN mhdu.diagnosis = 'Severe depressive episode without psychotic symptoms' THEN 1 ELSE NULL END) AS severe_depressive_episode_without_psychotic_symptoms,
		MAX (CASE WHEN mhdu.diagnosis = 'Somatoform disorders' THEN 1 ELSE NULL END) AS somatoform_disorders,
		MAX (CASE WHEN mhdu.diagnosis = 'Other' THEN 1 ELSE NULL END) AS other_mh
	FROM (SELECT patient_id, intake_encounter_id, diagnosis FROM last_mh_main_dx
	UNION
	SELECT patient_id, intake_encounter_id, diagnosis FROM last_mh_sec_dx) mhdu
	GROUP BY mhdu.patient_id, intake_encounter_id),
-- The following sub-tables create the active patient calculation by counting each day the cumulative admissions and exits from the cohort.
active_patients AS (
	SELECT
		c.intake_date, 
		c.discharge_date,
		CASE
			WHEN (ncddx.focal_epilepsy IS NOT NULL OR ncddx.generalised_epilepsy IS NOT NULL OR ncddx.unclassified_epilepsy IS NOT NULL OR ncddx.other_ncd IS NOT NULL) AND 
				mhdx.acute_transient_psychotic_disorder IS NULL AND mhdx.acute_stress_reaction IS NULL AND mhdx.adjustment_disorders IS NULL AND mhdx.anxiety_disorder IS NULL AND mhdx.bipolar_disorder IS NULL AND mhdx.childhood_emotional_disorder IS NULL AND mhdx.conduct_disorders IS NULL AND mhdx.delirium IS NULL AND mhdx.dementia IS NULL AND mhdx.dissociative_conversion_disorder IS NULL AND mhdx.dissociative_convulsions IS NULL AND mhdx.hyperkinetic_disorder IS NULL AND mhdx.intellectual_disability IS NULL AND mhdx.disorders_due_drug_psychoactive_substances IS NULL AND mhdx.disorders_due_alcohol IS NULL AND mhdx.mild_depressive_episode IS NULL AND mhdx.moderate_depressive_episode IS NULL AND mhdx.nonorganic_enuresis IS NULL AND mhdx.obsessive_compulsive_disorder IS NULL AND mhdx.panic_disorder IS NULL AND mhdx.pervasive_developmental_disorder IS NULL AND mhdx.postpartum_depression IS NULL AND mhdx.postpartum_psychosis IS NULL AND mhdx.ptsd IS NULL AND mhdx.schizophrenia IS NULL AND mhdx.severe_depressive_episode_with_psychotic_symptoms IS NULL AND mhdx.severe_depressive_episode_without_psychotic_symptoms IS NULL AND mhdx.somatoform_disorders IS NULL AND mhdx.other_mh IS NULL THEN 'Epilepsy'
			WHEN (mhdx.disorders_due_drug_psychoactive_substances IS NOT NULL OR mhdx.disorders_due_alcohol IS NOT NULL) AND 
				ncddx.focal_epilepsy IS NULL AND ncddx.generalised_epilepsy IS NULL AND ncddx.unclassified_epilepsy IS NULL AND ncddx.other_ncd IS NULL THEN 'Substance use disorders'
			WHEN (mhdx.acute_transient_psychotic_disorder IS NOT NULL OR mhdx.acute_stress_reaction IS NOT NULL OR mhdx.adjustment_disorders IS NOT NULL OR mhdx.anxiety_disorder IS NOT NULL OR mhdx.bipolar_disorder IS NOT NULL OR mhdx.childhood_emotional_disorder IS NOT NULL OR mhdx.conduct_disorders IS NOT NULL OR mhdx.delirium IS NOT NULL OR mhdx.dementia IS NOT NULL OR mhdx.dissociative_conversion_disorder IS NOT NULL OR mhdx.dissociative_convulsions IS NOT NULL OR mhdx.hyperkinetic_disorder IS NOT NULL OR mhdx.intellectual_disability IS NOT NULL OR mhdx.mild_depressive_episode IS NOT NULL OR mhdx.moderate_depressive_episode IS NOT NULL OR mhdx.nonorganic_enuresis IS NOT NULL OR mhdx.obsessive_compulsive_disorder IS NOT NULL OR mhdx.panic_disorder IS NOT NULL OR mhdx.pervasive_developmental_disorder IS NOT NULL OR mhdx.postpartum_depression IS NOT NULL OR mhdx.postpartum_psychosis IS NOT NULL OR mhdx.ptsd IS NOT NULL OR mhdx.schizophrenia IS NOT NULL OR mhdx.severe_depressive_episode_with_psychotic_symptoms IS NOT NULL OR mhdx.severe_depressive_episode_without_psychotic_symptoms IS NOT NULL OR mhdx.somatoform_disorders IS NOT NULL OR mhdx.other_mh IS NOT NULL) AND 
				ncddx.focal_epilepsy IS NULL AND ncddx.generalised_epilepsy IS NULL AND ncddx.unclassified_epilepsy IS NULL AND ncddx.other_ncd IS NULL THEN 'Mental health'
			WHEN (mhdx.acute_transient_psychotic_disorder IS NOT NULL OR mhdx.acute_stress_reaction IS NOT NULL OR mhdx.adjustment_disorders IS NOT NULL OR mhdx.anxiety_disorder IS NOT NULL OR mhdx.bipolar_disorder IS NOT NULL OR mhdx.childhood_emotional_disorder IS NOT NULL OR mhdx.conduct_disorders IS NOT NULL OR mhdx.delirium IS NOT NULL OR mhdx.dementia IS NOT NULL OR mhdx.dissociative_conversion_disorder IS NOT NULL OR mhdx.dissociative_convulsions IS NOT NULL OR mhdx.hyperkinetic_disorder IS NOT NULL OR mhdx.intellectual_disability IS NOT NULL OR mhdx.disorders_due_drug_psychoactive_substances IS NOT NULL OR mhdx.disorders_due_alcohol IS NOT NULL OR mhdx.mild_depressive_episode IS NOT NULL OR mhdx.moderate_depressive_episode IS NOT NULL OR mhdx.nonorganic_enuresis IS NOT NULL OR mhdx.obsessive_compulsive_disorder IS NOT NULL OR mhdx.panic_disorder IS NOT NULL OR mhdx.pervasive_developmental_disorder IS NOT NULL OR mhdx.postpartum_depression IS NOT NULL OR mhdx.postpartum_psychosis IS NOT NULL OR mhdx.ptsd IS NOT NULL OR mhdx.schizophrenia IS NOT NULL OR mhdx.severe_depressive_episode_with_psychotic_symptoms IS NOT NULL OR mhdx.severe_depressive_episode_without_psychotic_symptoms IS NOT NULL OR mhdx.somatoform_disorders IS NOT NULL OR mhdx.other_mh IS NOT NULL) AND 
				(ncddx.focal_epilepsy IS NOT NULL OR ncddx.generalised_epilepsy IS NOT NULL OR ncddx.unclassified_epilepsy IS NOT NULL OR ncddx.other_ncd IS NOT NULL) THEN 'Mental health/epilepsy'
			ELSE NULL 
		END AS cohort
	FROM cohort c
	LEFT OUTER JOIN last_ncd_diagnosis ncddx
		ON c.intake_encounter_id = ncddx.intake_encounter_id 
	LEFT OUTER JOIN last_mh_diagnosis mhdx
		ON c.intake_encounter_id = mhdx.intake_encounter_id),
range_values AS (
	SELECT 
		date_trunc('day',min(ap.intake_date)) AS minval,
		date_trunc('day',CURRENT_DATE) AS maxval
	FROM active_patients AS ap),
day_range AS (
	SELECT 
		generate_series(minval,maxval,'1 day'::interval) as day
	FROM range_values),   
daily_admissions_total AS (
	SELECT 
		date_trunc('day', ap.intake_date) AS day,
		count(*) AS patients
	FROM active_patients AS ap
	GROUP BY 1),
daily_admissions_mh AS (
	SELECT 
		date_trunc('day', ap.intake_date) AS day,
		count(*) AS patients
	FROM active_patients AS ap
	WHERE cohort = 'Mental health'
	GROUP BY 1),
daily_admissions_epi AS (
	SELECT 
		date_trunc('day', ap.intake_date) AS day,
		count(*) AS patients
	FROM active_patients AS ap
	WHERE cohort = 'Epilepsy'
	GROUP BY 1),
daily_admissions_mhepi AS (
	SELECT 
		date_trunc('day', ap.intake_date) AS day,
		count(*) AS patients
	FROM active_patients AS ap
	WHERE cohort = 'Mental health/epilepsy'
	GROUP BY 1),
daily_admissions_sud AS (
	SELECT 
		date_trunc('day', ap.intake_date) AS day,
		count(*) AS patients
	FROM active_patients AS ap
	WHERE cohort = 'Substance use disorders'
	GROUP BY 1),
daily_exits_total AS (
	SELECT
		date_trunc('day',ap.discharge_date) AS day,
		count(*) AS patients
	FROM active_patients AS ap
	GROUP BY 1),
daily_exits_mh AS (
	SELECT
		date_trunc('day',ap.discharge_date) AS day,
		count(*) AS patients
	FROM active_patients AS ap
	WHERE cohort = 'Mental health'
	GROUP BY 1),
daily_exits_epi AS (
	SELECT
		date_trunc('day',ap.discharge_date) AS day,
		count(*) AS patients
	FROM active_patients AS ap
	WHERE cohort = 'Epilepsy'
	GROUP BY 1),
daily_exits_mhepi AS (
	SELECT
		date_trunc('day',ap.discharge_date) AS day,
		count(*) AS patients
	FROM active_patients AS ap
	WHERE cohort = 'Mental health/epilepsy'
	GROUP BY 1),
daily_exits_sud AS (
	SELECT
		date_trunc('day',ap.discharge_date) AS day,
		count(*) AS patients
	FROM active_patients AS ap
	WHERE cohort = 'Substance use disorders'
	GROUP BY 1),
daily_active_patients AS (
	SELECT 
		dr.day as reporting_day,
		sum(dat.patients) over (order by dr.day asc rows between unbounded preceding and current row) AS cumulative_admissions_total,
		sum(damh.patients) over (order by dr.day asc rows between unbounded preceding and current row) AS cumulative_admissions_mh,
		sum(dae.patients) over (order by dr.day asc rows between unbounded preceding and current row) AS cumulative_admissions_epi,
		sum(damhe.patients) over (order by dr.day asc rows between unbounded preceding and current row) AS cumulative_admissions_mhepi,
		sum(dasud.patients) over (order by dr.day asc rows between unbounded preceding and current row) AS cumulative_admissions_sud,
		CASE
		    WHEN sum(det.patients) over (order by dr.day asc rows between unbounded preceding and current row) IS NULL
		    THEN 0
		    ELSE sum(det.patients) over (order by dr.day asc rows between unbounded preceding and current row) 
		END AS cumulative_exits_total, 
		CASE
		    WHEN sum(demh.patients) over (order by dr.day asc rows between unbounded preceding and current row) IS NULL
		    THEN 0
		    ELSE sum(demh.patients) over (order by dr.day asc rows between unbounded preceding and current row) 
		END AS cumulative_exits_mh, 
		CASE
		    WHEN sum(dee.patients) over (order by dr.day asc rows between unbounded preceding and current row) IS NULL
		    THEN 0
		    ELSE sum(dee.patients) over (order by dr.day asc rows between unbounded preceding and current row) 
		END AS cumulative_exits_epi, 
		CASE
		    WHEN sum(demhe.patients) over (order by dr.day asc rows between unbounded preceding and current row) IS NULL
		    THEN 0
		    ELSE sum(demhe.patients) over (order by dr.day asc rows between unbounded preceding and current row) 
		END AS cumulative_exits_mhepi, 
		CASE
		    WHEN sum(desud.patients) over (order by dr.day asc rows between unbounded preceding and current row) IS NULL
		    THEN 0
		    ELSE sum(desud.patients) over (order by dr.day asc rows between unbounded preceding and current row) 
		END AS cumulative_exits_sud, 
		CASE
		    WHEN sum(det.patients) over (order by dr.day asc rows between unbounded preceding and current row) IS NULL 
			THEN sum(dat.patients) over (order by dr.day asc rows between unbounded preceding and current row)
		    ELSE (sum(dat.patients) over (order by dr.day asc rows between unbounded preceding and current row)-
				sum(det.patients) over (order by dr.day asc rows between unbounded preceding and current row)) 
		END AS active_patients_total,
		CASE
		    WHEN sum(demh.patients) over (order by dr.day asc rows between unbounded preceding and current row) IS NULL 
			THEN sum(damh.patients) over (order by dr.day asc rows between unbounded preceding and current row)
		    ELSE (sum(damh.patients) over (order by dr.day asc rows between unbounded preceding and current row)-
				sum(demh.patients) over (order by dr.day asc rows between unbounded preceding and current row)) 
		END AS active_patients_mh,
		CASE
		    WHEN sum(dee.patients) over (order by dr.day asc rows between unbounded preceding and current row) IS NULL 
			THEN sum(dae.patients) over (order by dr.day asc rows between unbounded preceding and current row)
		    ELSE (sum(dae.patients) over (order by dr.day asc rows between unbounded preceding and current row)-
				sum(dee.patients) over (order by dr.day asc rows between unbounded preceding and current row)) 
		END AS active_patients_epi,
		CASE
		    WHEN sum(demhe.patients) over (order by dr.day asc rows between unbounded preceding and current row) IS NULL 
			THEN sum(damhe.patients) over (order by dr.day asc rows between unbounded preceding and current row)
		    ELSE (sum(damhe.patients) over (order by dr.day asc rows between unbounded preceding and current row)-
				sum(demhe.patients) over (order by dr.day asc rows between unbounded preceding and current row)) 
		END AS active_patients_mhepi,
		CASE
		    WHEN sum(desud.patients) over (order by dr.day asc rows between unbounded preceding and current row) IS NULL 
			THEN sum(dasud.patients) over (order by dr.day asc rows between unbounded preceding and current row)
		    ELSE (sum(dasud.patients) over (order by dr.day asc rows between unbounded preceding and current row)-
				sum(desud.patients) over (order by dr.day asc rows between unbounded preceding and current row)) 
		END AS active_patients_sud,
		CASE 
			WHEN date(dr.day)::date = (date_trunc('MONTH', dr.day) + INTERVAL '1 MONTH - 1 day')::date THEN 1
		END AS last_day_of_month
	FROM day_range dr
	LEFT OUTER JOIN daily_admissions_total dat ON dr.day = dat.day
	LEFT OUTER JOIN daily_admissions_mh damh ON dr.day = damh.day
	LEFT OUTER JOIN daily_admissions_epi dae ON dr.day = dae.day
	LEFT OUTER JOIN daily_admissions_mhepi damhe ON dr.day = damhe.day
	LEFT OUTER JOIN daily_admissions_sud dasud ON dr.day = dasud.day
	LEFT OUTER JOIN daily_exits_total det ON dr.day = det.day
	LEFT OUTER JOIN daily_exits_mh demh ON dr.day = demh.day
	LEFT OUTER JOIN daily_exits_epi dee ON dr.day = dee.day
	LEFT OUTER JOIN daily_exits_mhepi demhe ON dr.day = demhe.day
	LEFT OUTER JOIN daily_exits_sud desud ON dr.day = desud.day)
-- Main query --
SELECT 
	dap.reporting_day,
	dap.active_patients_total,
	dap.active_patients_mh,
	dap.active_patients_epi,
	dap.active_patients_mhepi,
	dap.active_patients_sud
FROM daily_active_patients dap
WHERE dap.last_day_of_month = 1 and dap.reporting_day > date_trunc('MONTH', CURRENT_DATE) - INTERVAL '1 year'
GROUP BY dap.reporting_day, dap.active_patients_total, dap.active_patients_mh, dap.active_patients_epi, dap.active_patients_mhepi, dap.active_patients_sud;