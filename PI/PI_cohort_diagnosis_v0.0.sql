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
-- The Mental Health diagnosis sub-tables pivot mental health diagnosis data horizontally from the Psychiatrist mhGap initial and follow-up forms. Only the last diagnoses reported are present. 
last_mh_main_dx_cte AS (
	SELECT 
		DISTINCT ON (eec.patient_id, eec.entry_encounter_id) eec.patient_id,
		eec.entry_encounter_id,
		eec.intake_date, 
		eec.discharge_encounter_id,
		eec.discharge_date,
		mmhd.date,
		CASE WHEN mmhd.main_diagnosis = 'None' THEN NULL WHEN mmhd.main_diagnosis = 'Other' THEN 'Other mental health diagnosis' WHEN mmhd.main_diagnosis != 'Other' OR mmhd.main_diagnosis != 'None' THEN mmhd.main_diagnosis ELSE NULL END AS diagnosis
	FROM entry_exit_cte eec
	LEFT OUTER JOIN (
		SELECT patient_id, date::date, main_diagnosis FROM psychiatrist_mhgap_initial_assessment
		UNION
		SELECT patient_id, date::date, main_diagnosis FROM psychiatrist_mhgap_follow_up) mmhd
		ON eec.patient_id = mmhd.patient_id AND eec.intake_date <= mmhd.date::date AND CASE WHEN eec.discharge_date IS NOT NULL THEN eec.discharge_date ELSE current_date END >= mmhd.date::date
	WHERE mmhd.main_diagnosis IS NOT NULL 
	GROUP BY eec.patient_id, eec.entry_encounter_id, eec.intake_date, eec.discharge_encounter_id, eec.discharge_date, mmhd.date::date, mmhd.main_diagnosis 
	ORDER BY eec.patient_id, eec.entry_encounter_id, eec.intake_date, mmhd.date::date DESC),
last_mh_sec_dx_cte AS (
	SELECT 
		DISTINCT ON (eec.patient_id, eec.entry_encounter_id) eec.patient_id,
		eec.entry_encounter_id,
		eec.intake_date, 
		eec.discharge_encounter_id,
		eec.discharge_date,
		mmhd.date,
		CASE WHEN mmhd.secondary_diagnosis = 'None' THEN NULL WHEN mmhd.secondary_diagnosis = 'Other' THEN 'Other mental health diagnosis' WHEN mmhd.secondary_diagnosis != 'Other' OR mmhd.secondary_diagnosis != 'None' THEN mmhd.secondary_diagnosis ELSE NULL END AS diagnosis
	FROM entry_exit_cte eec
	LEFT OUTER JOIN (
		SELECT patient_id, date::date, secondary_diagnosis FROM psychiatrist_mhgap_initial_assessment
		UNION
		SELECT patient_id, date::date, secondary_diagnosis FROM psychiatrist_mhgap_follow_up) mmhd
		ON eec.patient_id = mmhd.patient_id AND eec.intake_date <= mmhd.date::date AND CASE WHEN eec.discharge_date IS NOT NULL THEN eec.discharge_date ELSE current_date END >= mmhd.date::date
	WHERE mmhd.secondary_diagnosis IS NOT NULL 
	GROUP BY eec.patient_id, eec.entry_encounter_id, eec.intake_date, eec.discharge_encounter_id, eec.discharge_date, mmhd.date::date, mmhd.secondary_diagnosis 
	ORDER BY eec.patient_id, eec.entry_encounter_id, eec.intake_date, mmhd.date::date DESC),
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
-- The visit location sub-table finds the last visit location reported across all clinical consultaiton/session forms.
last_visit_location_cte AS (	
	SELECT 
		DISTINCT ON (eec.patient_id, eec.entry_encounter_id, eec.intake_date, eec.discharge_date) eec.entry_encounter_id,
		vlc.visit_location AS visit_location
	FROM entry_exit_cte eec
	LEFT OUTER JOIN (
		SELECT pcia.date::date, pcia.patient_id, pcia.visit_location FROM psy_counselors_initial_assessment pcia WHERE pcia.visit_location IS NOT NULL 
		UNION 
		SELECT pmia.date::date, pmia.patient_id, pmia.visit_location FROM psychiatrist_mhgap_initial_assessment pmia WHERE pmia.visit_location IS NOT NULL 
		UNION
		SELECT pcfu.date::date, pcfu.patient_id, pcfu.visit_location FROM psy_counselors_follow_up pcfu WHERE pcfu.visit_location IS NOT NULL 
		UNION
		SELECT pmfu.date::date, pmfu.patient_id, pmfu.visit_location FROM psychiatrist_mhgap_follow_up pmfu WHERE pmfu.visit_location IS NOT NULL
		UNION
		SELECT mhd.discharge_date AS date, mhd.patient_id, mhd.visit_location FROM mental_health_discharge mhd WHERE mhd.visit_location IS NOT NULL) vlc
		ON eec.patient_id = vlc.patient_id
	WHERE vlc.date >= eec.intake_date AND (vlc.date <= eec.discharge_date OR eec.discharge_date IS NULL)
	GROUP BY eec.patient_id, eec.entry_encounter_id, eec.intake_date, eec.discharge_date, vlc.date, vlc.visit_location
	ORDER BY eec.patient_id, eec.entry_encounter_id, eec.intake_date, eec.discharge_date, vlc.date DESC),
-- The all diagnosis sub-table combines a list of the last reported mental health diagnosis for each cohort entry.
all_diagnosis AS (
	SELECT mdx.patient_id, mdx.entry_encounter_id, mdx.date, mdx.diagnosis 
	FROM last_mh_main_dx_cte mdx
	UNION
	SELECT sdx.patient_id, sdx.entry_encounter_id, sdx.date, sdx.diagnosis 
	FROM last_mh_sec_dx_cte sdx)
-- Main query --
SELECT 
	pi."Patient_Identifier",
	eec.patient_id,
	eec.entry_encounter_id,
	pdd.age AS age_current,
	CASE 
		WHEN pdd.age::int <= 3 THEN '0-3'
		WHEN pdd.age::int >= 4 AND pdd.age::int <= 7 THEN '04-07'
		WHEN pdd.age::int >= 8 AND pdd.age::int <= 14 THEN '08-14'
		WHEN pdd.age::int >= 15 AND pdd.age::int <= 17 THEN '15-17'
		WHEN pdd.age::int >= 18 THEN '18+'
		ELSE NULL
	END AS age_group_current,
	EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))) AS age_admission,
	CASE 
		WHEN EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))))::int <= 3 THEN '0-3'
		WHEN EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))))::int >= 4 AND EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))) <= 7 THEN '04-07'
		WHEN EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))))::int >= 8 AND EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))) <= 14 THEN '08-14'
		WHEN EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))))::int >= 15 AND EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))) <= 17 THEN '15-17'
		WHEN EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))))::int >= 18 THEN '18+'
		ELSE NULL
	END AS age_group_admission,
	pdd.gender,
	eec.intake_date, 
	CASE 
		WHEN fpia.date IS NOT NULL THEN fpia.date
		WHEN fpia.date IS NULL THEN fcia.date
		ELSE NULL
	END	AS enrollment_date,
	eec.discharge_date,
	CASE 
		WHEN fpia.date IS NULL AND fcia.date IS NULL AND eec.discharge_date IS NULL THEN 'waiting list'
		WHEN (fpia.date IS NOT NULL OR fcia.date IS NOT NULL) AND eec.discharge_date IS NULL THEN 'in cohort'
		WHEN (fpia.date IS NOT NULL OR fcia.date IS NOT NULL) AND eec.discharge_date IS NOT NULL THEN 'discharge'
	END AS status,
	mhi.visit_location AS entry_visit_location,
	lvlc.visit_location,
	adx.diagnosis
FROM all_diagnosis adx
LEFT OUTER JOIN entry_exit_cte eec
	ON adx.entry_encounter_id = eec.entry_encounter_id
LEFT OUTER JOIN first_psy_initial_assessment fpia 
	ON adx.entry_encounter_id = fpia.entry_encounter_id
LEFT OUTER JOIN first_clinician_initial_assessment fcia 
	ON adx.entry_encounter_id = fcia.entry_encounter_id
LEFT OUTER JOIN patient_identifier pi 
	ON adx.patient_id = pi.patient_id
LEFT OUTER JOIN person_details_default pdd 
	ON eec.patient_id = pdd.person_id
LEFT OUTER JOIN patient_encounter_details_default ped 
	ON eec.entry_encounter_id = ped.encounter_id
LEFT OUTER JOIN mental_health_intake mhi
	ON eec.entry_encounter_id = mhi.encounter_id
LEFT OUTER JOIN last_mh_diagnosis_cte mhdc 
	ON adx.entry_encounter_id = mhdc.entry_encounter_id
LEFT OUTER JOIN last_visit_location_cte lvlc 
	ON adx.entry_encounter_id = lvlc.entry_encounter_id;