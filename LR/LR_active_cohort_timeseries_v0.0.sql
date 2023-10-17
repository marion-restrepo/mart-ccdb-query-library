-- The first 3 sub-tables of the query build the frame for monitoring patient entry to and exit from the cohort/active treatment. The table is build by listing all entry dates as recorded in the MH intake form. For each exit date is matched to an intake following the logic that is occurs after the entry date but not before the next entry date, in the case that a patient enters the cohort more than once.
WITH entry_cte_2 AS (
	SELECT patient_id, encounter_id AS entry_encounter_id, date::date AS entry_date, CONCAT(patient_id, ROW_NUMBER () OVER (PARTITION BY patient_id ORDER BY date)) AS entry_id_2, 1 AS one
	FROM mental_health_intake),
entry_cte_1 AS (
	SELECT patient_id, entry_encounter_id, entry_date, entry_id_2::int+one AS entry_id_1
	FROM entry_cte_2),
entry_exit_cte AS (
	SELECT ec1.patient_id, ec1.entry_encounter_id, ec1.entry_date, mhd.discharge_date::date, mhd.encounter_id AS discharge_encounter_id
	FROM entry_cte_1 ec1
	LEFT OUTER JOIN entry_cte_2 ec2
		ON ec1.entry_id_1::int = ec2.entry_id_2::int
	LEFT OUTER JOIN (SELECT patient_id, discharge_date, encounter_id FROM mental_health_discharge) mhd
		ON ec1.patient_id = mhd.patient_id AND mhd.discharge_date >= ec1.entry_date AND (mhd.discharge_date < ec2.entry_date OR ec2.entry_date IS NULL)),
-- The NCD diagnosis sub-tables pivot NCD diagnosis data horizontally from the NCD form. Only the last diagnoses reported are present. 
ncd_diagnosis_pivot_cte AS (
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
last_ncd_diagnosis_cte AS (
	SELECT 
		DISTINCT ON (eec.patient_id, eec.entry_encounter_id) eec.patient_id,
		eec.entry_encounter_id,
		eec.entry_date, 
		eec.discharge_encounter_id,
		eec.discharge_date, 
		ndpc.date::date,
		ndpc.focal_epilepsy,
		ndpc.generalised_epilepsy,
		ndpc.unclassified_epilepsy,
		ndpc.other_ncd
	FROM entry_exit_cte eec
	LEFT OUTER JOIN ncd_diagnosis_pivot_cte ndpc 
		ON eec.patient_id = ndpc.patient_id AND eec.entry_date <= ndpc.date AND CASE WHEN eec.discharge_date IS NOT NULL THEN eec.discharge_date ELSE current_date END >= ndpc.date
	GROUP BY eec.patient_id, eec.entry_encounter_id, eec.entry_date, eec.discharge_encounter_id, eec.discharge_date, ndpc.date, ndpc.focal_epilepsy, ndpc.generalised_epilepsy, ndpc.unclassified_epilepsy, ndpc.other_ncd
	ORDER BY eec.patient_id, eec.entry_encounter_id, eec.entry_date, ndpc.date DESC),
-- The Mental Health diagnosis sub-tables pivot mental health diagnosis data horizontally from the Psychiatrist mhGap initial and follow-up forms. Only the last diagnoses reported are present. 
last_mh_main_dx_cte AS (
	SELECT 
		DISTINCT ON (eec.patient_id, eec.entry_encounter_id) eec.patient_id,
		eec.entry_encounter_id,
		eec.entry_date, 
		eec.discharge_encounter_id,
		eec.discharge_date,
		mmhd.date,
		mmhd.main_diagnosis AS diagnosis
	FROM entry_exit_cte eec
	LEFT OUTER JOIN (
		SELECT patient_id, date::date, main_diagnosis FROM psychiatrist_mhgap_initial_assessment
		UNION
		SELECT patient_id, date::date, main_diagnosis FROM psychiatrist_mhgap_follow_up) mmhd
		ON eec.patient_id = mmhd.patient_id AND eec.entry_date <= mmhd.date::date AND CASE WHEN eec.discharge_date IS NOT NULL THEN eec.discharge_date ELSE current_date END >= mmhd.date::date
	WHERE mmhd.main_diagnosis IS NOT NULL 
	GROUP BY eec.patient_id, eec.entry_encounter_id, eec.entry_date, eec.discharge_encounter_id, eec.discharge_date, mmhd.date::date,	mmhd.main_diagnosis 
	ORDER BY eec.patient_id, eec.entry_encounter_id, eec.entry_date, mmhd.date::date DESC),
last_mh_sec_dx_cte AS (
	SELECT 
		DISTINCT ON (eec.patient_id, eec.entry_encounter_id) eec.patient_id,
		eec.entry_encounter_id,
		eec.entry_date, 
		eec.discharge_encounter_id,
		eec.discharge_date,
		mmhd.date,
		mmhd.secondary_diagnosis AS diagnosis
	FROM entry_exit_cte eec
	LEFT OUTER JOIN (
		SELECT patient_id, date::date, secondary_diagnosis FROM psychiatrist_mhgap_initial_assessment
		UNION
		SELECT patient_id, date::date, secondary_diagnosis FROM psychiatrist_mhgap_follow_up) mmhd
		ON eec.patient_id = mmhd.patient_id AND eec.entry_date <= mmhd.date::date AND CASE WHEN eec.discharge_date IS NOT NULL THEN eec.discharge_date ELSE current_date END >= mmhd.date::date
	WHERE mmhd.secondary_diagnosis IS NOT NULL 
	GROUP BY eec.patient_id, eec.entry_encounter_id, eec.entry_date, eec.discharge_encounter_id, eec.discharge_date, mmhd.date::date, mmhd.secondary_diagnosis 
	ORDER BY eec.patient_id, eec.entry_encounter_id, eec.entry_date, mmhd.date::date DESC),
last_mh_diagnosis_cte AS (
	SELECT 
		DISTINCT ON (mhdu.patient_id, entry_encounter_id) mhdu.patient_id, 
		entry_encounter_id,
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
	FROM (SELECT patient_id, entry_encounter_id, diagnosis FROM last_mh_main_dx_cte
	UNION
	SELECT patient_id, entry_encounter_id, diagnosis FROM last_mh_sec_dx_cte) mhdu
	GROUP BY mhdu.patient_id, entry_encounter_id),
-- The following sub-tables create the active patient calculation by counting each day the cumulative admissions and exits from the cohort.
active_patients AS (
	SELECT
		eec.entry_date, 
		eec.discharge_date,
		CASE
			WHEN (ncddc.focal_epilepsy IS NOT NULL OR ncddc.generalised_epilepsy IS NOT NULL OR ncddc.unclassified_epilepsy IS NOT NULL OR ncddc.other_ncd IS NOT NULL) AND 
				mhdc.acute_transient_psychotic_disorder IS NULL AND mhdc.acute_stress_reaction IS NULL AND mhdc.adjustment_disorders IS NULL AND mhdc.anxiety_disorder IS NULL AND mhdc.bipolar_disorder IS NULL AND mhdc.childhood_emotional_disorder IS NULL AND mhdc.conduct_disorders IS NULL AND mhdc.delirium IS NULL AND mhdc.dementia IS NULL AND mhdc.dissociative_conversion_disorder IS NULL AND mhdc.dissociative_convulsions IS NULL AND mhdc.hyperkinetic_disorder IS NULL AND mhdc.intellectual_disability IS NULL AND mhdc.disorders_due_drug_psychoactive_substances IS NULL AND mhdc.disorders_due_alcohol IS NULL AND mhdc.mild_depressive_episode IS NULL AND mhdc.moderate_depressive_episode IS NULL AND mhdc.nonorganic_enuresis IS NULL AND mhdc.obsessive_compulsive_disorder IS NULL AND mhdc.panic_disorder IS NULL AND mhdc.pervasive_developmental_disorder IS NULL AND mhdc.postpartum_depression IS NULL AND mhdc.postpartum_psychosis IS NULL AND mhdc.ptsd IS NULL AND mhdc.schizophrenia IS NULL AND mhdc.severe_depressive_episode_with_psychotic_symptoms IS NULL AND mhdc.severe_depressive_episode_without_psychotic_symptoms IS NULL AND mhdc.somatoform_disorders IS NULL AND mhdc.other_mh IS NULL THEN 'Epilepsy'
			WHEN (mhdc.disorders_due_drug_psychoactive_substances IS NOT NULL OR mhdc.disorders_due_alcohol IS NOT NULL) AND 
				ncddc.focal_epilepsy IS NULL AND ncddc.generalised_epilepsy IS NULL AND ncddc.unclassified_epilepsy IS NULL AND ncddc.other_ncd IS NULL THEN 'Substance use disorders'
			WHEN (mhdc.acute_transient_psychotic_disorder IS NOT NULL OR mhdc.acute_stress_reaction IS NOT NULL OR mhdc.adjustment_disorders IS NOT NULL OR mhdc.anxiety_disorder IS NOT NULL OR mhdc.bipolar_disorder IS NOT NULL OR mhdc.childhood_emotional_disorder IS NOT NULL OR mhdc.conduct_disorders IS NOT NULL OR mhdc.delirium IS NOT NULL OR mhdc.dementia IS NOT NULL OR mhdc.dissociative_conversion_disorder IS NOT NULL OR mhdc.dissociative_convulsions IS NOT NULL OR mhdc.hyperkinetic_disorder IS NOT NULL OR mhdc.intellectual_disability IS NOT NULL OR mhdc.mild_depressive_episode IS NOT NULL OR mhdc.moderate_depressive_episode IS NOT NULL OR mhdc.nonorganic_enuresis IS NOT NULL OR mhdc.obsessive_compulsive_disorder IS NOT NULL OR mhdc.panic_disorder IS NOT NULL OR mhdc.pervasive_developmental_disorder IS NOT NULL OR mhdc.postpartum_depression IS NOT NULL OR mhdc.postpartum_psychosis IS NOT NULL OR mhdc.ptsd IS NOT NULL OR mhdc.schizophrenia IS NOT NULL OR mhdc.severe_depressive_episode_with_psychotic_symptoms IS NOT NULL OR mhdc.severe_depressive_episode_without_psychotic_symptoms IS NOT NULL OR mhdc.somatoform_disorders IS NOT NULL OR mhdc.other_mh IS NOT NULL) AND 
				ncddc.focal_epilepsy IS NULL AND ncddc.generalised_epilepsy IS NULL AND ncddc.unclassified_epilepsy IS NULL AND ncddc.other_ncd IS NULL THEN 'Mental health'
			WHEN (mhdc.acute_transient_psychotic_disorder IS NOT NULL OR mhdc.acute_stress_reaction IS NOT NULL OR mhdc.adjustment_disorders IS NOT NULL OR mhdc.anxiety_disorder IS NOT NULL OR mhdc.bipolar_disorder IS NOT NULL OR mhdc.childhood_emotional_disorder IS NOT NULL OR mhdc.conduct_disorders IS NOT NULL OR mhdc.delirium IS NOT NULL OR mhdc.dementia IS NOT NULL OR mhdc.dissociative_conversion_disorder IS NOT NULL OR mhdc.dissociative_convulsions IS NOT NULL OR mhdc.hyperkinetic_disorder IS NOT NULL OR mhdc.intellectual_disability IS NOT NULL OR mhdc.disorders_due_drug_psychoactive_substances IS NOT NULL OR mhdc.disorders_due_alcohol IS NOT NULL OR mhdc.mild_depressive_episode IS NOT NULL OR mhdc.moderate_depressive_episode IS NOT NULL OR mhdc.nonorganic_enuresis IS NOT NULL OR mhdc.obsessive_compulsive_disorder IS NOT NULL OR mhdc.panic_disorder IS NOT NULL OR mhdc.pervasive_developmental_disorder IS NOT NULL OR mhdc.postpartum_depression IS NOT NULL OR mhdc.postpartum_psychosis IS NOT NULL OR mhdc.ptsd IS NOT NULL OR mhdc.schizophrenia IS NOT NULL OR mhdc.severe_depressive_episode_with_psychotic_symptoms IS NOT NULL OR mhdc.severe_depressive_episode_without_psychotic_symptoms IS NOT NULL OR mhdc.somatoform_disorders IS NOT NULL OR mhdc.other_mh IS NOT NULL) AND 
				(ncddc.focal_epilepsy IS NOT NULL OR ncddc.generalised_epilepsy IS NOT NULL OR ncddc.unclassified_epilepsy IS NOT NULL OR ncddc.other_ncd IS NOT NULL) THEN 'Mental health/epilepsy'
			ELSE NULL 
		END AS cohort,
	FROM entry_exit_cte eec
	LEFT OUTER JOIN last_ncd_diagnosis_cte ncddc
		ON eec.entry_encounter_id = ncddc.entry_encounter_id 
	LEFT OUTER JOIN last_mh_diagnosis_cte mhdc
		ON eec.entry_encounter_id = mhdc.entry_encounter_id),
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
daily_admissions_mh AS (
	SELECT 
		date_trunc('day', ap.entry_date) AS day,
		count(*) AS patients
	FROM active_patients AS ap
	WHERE cohort = 'Mental health'
	GROUP BY 1),
daily_admissions_epi AS (
	SELECT 
		date_trunc('day', ap.entry_date) AS day,
		count(*) AS patients
	FROM active_patients AS ap
	WHERE cohort = 'Epilepsy'
	GROUP BY 1),
daily_admissions_mhepi AS (
	SELECT 
		date_trunc('day', ap.entry_date) AS day,
		count(*) AS patients
	FROM active_patients AS ap
	WHERE cohort = 'Mental health/epilepsy'
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
daily_active_patients AS (
	SELECT 
		dr.day as reporting_day,
		sum(dat.patients) over (order by dr.day asc rows between unbounded preceding and current row) AS cumulative_admissions_total,
		sum(damh.patients) over (order by dr.day asc rows between unbounded preceding and current row) AS cumulative_admissions_mh,
		sum(dae.patients) over (order by dr.day asc rows between unbounded preceding and current row) AS cumulative_admissions_epi,
		sum(damhe.patients) over (order by dr.day asc rows between unbounded preceding and current row) AS cumulative_admissions_mhepi,
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
			WHEN date(dr.day)::date = (date_trunc('MONTH', dr.day) + INTERVAL '1 MONTH - 1 day')::date THEN 1
		END AS last_day_of_month
	FROM day_range dr
	LEFT OUTER JOIN daily_admissions_total dat ON dr.day = dat.day
	LEFT OUTER JOIN daily_admissions_mh damh ON dr.day = damh.day
	LEFT OUTER JOIN daily_admissions_epi dae ON dr.day = dae.day
	LEFT OUTER JOIN daily_admissions_mhepi damhe ON dr.day = damhe.day
	LEFT OUTER JOIN daily_exits_total det ON dr.day = det.day
	LEFT OUTER JOIN daily_exits_mh demh ON dr.day = demh.day
	LEFT OUTER JOIN daily_exits_epi dee ON dr.day = dee.day
	LEFT OUTER JOIN daily_exits_mhepi demhe ON dr.day = demhe.day)
-- Main query --
SELECT 
	dap.reporting_day,
	dap.active_patients_total,
	dap.active_patients_mh,
	dap.active_patients_epi,
	dap.active_patients_mhepi
FROM daily_active_patients dap
WHERE dap.last_day_of_month = 1 and dap.reporting_day > date_trunc('MONTH', CURRENT_DATE) - INTERVAL '1 year'
GROUP BY dap.reporting_day, dap.active_patients_total, dap.active_patients_mh, dap.active_patients_epi, dap.active_patients_mhepi;