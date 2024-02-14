-- The first CTE build the frame for patients entering and exiting the cohort. This frame is based on the MH intake form and the MH discharge form. The query takes all intake dates and matches discharge dates if the discharge date falls between the intake date and the next intake date (if present).
WITH intake AS (
	SELECT 
		patient_id, encounter_id AS intake_encounter_id, date::date AS intake_date, DENSE_RANK () OVER (PARTITION BY patient_id ORDER BY date) AS intake_order, LEAD (date::date) OVER (PARTITION BY patient_id ORDER BY date) AS next_intake_date
	FROM mental_health_intake),
cohort AS (
	SELECT
		i.patient_id, i.intake_encounter_id, i.intake_date, CASE WHEN i.intake_order > 1 THEN 'Yes' END readmission, mhd.encounter_id AS discharge_encounter_id, mhd.discharge_date::date
	FROM intake i
	LEFT JOIN mental_health_discharge mhd 
		ON i.patient_id = mhd.patient_id AND mhd.discharge_date >= i.intake_date AND (mhd.discharge_date < i.next_intake_date OR i.next_intake_date IS NULL)),
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
	GROUP BY c.patient_id, c.intake_encounter_id, c.intake_date, c.discharge_encounter_id, c.discharge_date, mmhd.date::date, mmhd.main_diagnosis 
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
		MAX (CASE WHEN mhdu.diagnosis = 'Focal epilepsy' THEN 1 ELSE NULL END) AS focal_epilepsy,
		MAX (CASE WHEN mhdu.diagnosis = 'Generalised epilepsy' THEN 1 ELSE NULL END) AS generalised_epilepsy,
		MAX (CASE WHEN mhdu.diagnosis = 'Unclassified epilepsy' THEN 1 ELSE NULL END) AS unclassified_epilepsy,
		MAX (CASE WHEN mhdu.diagnosis = 'Other' THEN 1 ELSE NULL END) AS other_mh
	FROM (SELECT patient_id, intake_encounter_id, diagnosis FROM last_mh_main_dx
	UNION
	SELECT patient_id, intake_encounter_id, diagnosis FROM last_mh_sec_dx) mhdu
	GROUP BY mhdu.patient_id, intake_encounter_id),	
-- The Consulations sub-table creates a master table of all consultations/sessions as reported by the clinical forms.
consultations_cte AS (
	SELECT
		pcia.date::date,
		pcia.patient_id,
		pcia.visit_location,
		pcia.intervention_setting,
		'Individual session' AS type_of_activity,
		'Initial' AS visit_type,
		'Counselor' AS provider_type,
		pcia.encounter_id
	FROM psy_counselors_initial_assessment pcia 
	UNION
	SELECT
		pmia.date::date,
		pmia.patient_id,
		pmia.visit_location,
		pmia.intervention_setting,
		'Individual session' AS type_of_activity,
		'Initial' AS visit_type,
		'Psychiatrist' AS provider_type,
		pmia.encounter_id
	FROM psychiatrist_mhgap_initial_assessment pmia
	UNION
	SELECT
		pcfu.date::date,
		pcfu.patient_id,
		pcfu.visit_location,
		pcfu.intervention_setting,
		pcfu.type_of_activity,
		'Follow up' AS visit_type,
		'Counselor' AS provider_type,
		pcfu.encounter_id
	FROM psy_counselors_follow_up pcfu 
	UNION
	SELECT 
		pmfu.date::date,
		pmfu.patient_id,
		pmfu.visit_location,
		pmfu.intervention_setting,
		pmfu.type_of_activity,
		'Follow up' AS visit_type,
		'Psychiatrist' AS provider_type,
		pmfu.encounter_id
	FROM psychiatrist_mhgap_follow_up pmfu)
-- Main query --
SELECT 
	DISTINCT ON (cc.patient_id, cc.date::date, cc.visit_location, cc.intervention_setting, cc.type_of_activity, cc.visit_type, cc.provider_type) cc.patient_id,
	pi."Patient_Identifier",
	c.intake_encounter_id,
	pdd.age AS age_current,
	CASE 
		WHEN pdd.age::int <= 3 THEN '0-3'
		WHEN pdd.age::int >= 4 AND pdd.age::int <= 7 THEN '04-07'
		WHEN pdd.age::int >= 8 AND pdd.age::int <= 14 THEN '08-14'
		WHEN pdd.age::int >= 15 AND pdd.age::int <= 17 THEN '15-17'
		WHEN pdd.age::int >= 18 AND pdd.age::int <= 59 THEN '18-59'
		WHEN pdd.age::int >= 60 THEN '60+'
		ELSE NULL
	END AS age_group_current,
	pdd.gender,
	pad.state_province AS governorate, 
	pad.city_village AS community_village,
	pad.address2 AS area,
	CASE 
		WHEN (mhdx.acute_transient_psychotic_disorder IS NULL AND mhdx.acute_stress_reaction IS NULL AND mhdx.adjustment_disorders IS NULL AND mhdx.anxiety_disorder IS NULL AND mhdx.bipolar_disorder IS NULL AND mhdx.childhood_emotional_disorder IS NULL AND mhdx.conduct_disorders IS NULL AND mhdx.delirium IS NULL AND mhdx.dementia IS NULL AND mhdx.dissociative_conversion_disorder IS NULL AND mhdx.dissociative_convulsions IS NULL AND mhdx.hyperkinetic_disorder IS NULL AND mhdx.intellectual_disability IS NULL AND mhdx.disorders_due_drug_psychoactive_substances IS NULL AND mhdx.disorders_due_alcohol IS NULL AND mhdx.mild_depressive_episode IS NULL AND mhdx.moderate_depressive_episode IS NULL AND mhdx.nonorganic_enuresis IS NULL AND mhdx.obsessive_compulsive_disorder IS NULL AND mhdx.panic_disorder IS NULL AND mhdx.pervasive_developmental_disorder IS NULL AND mhdx.postpartum_depression IS NULL AND mhdx.postpartum_psychosis IS NULL AND mhdx.ptsd IS NULL AND mhdx.schizophrenia IS NULL AND mhdx.severe_depressive_episode_with_psychotic_symptoms IS NULL AND mhdx.severe_depressive_episode_without_psychotic_symptoms IS NULL AND mhdx.somatoform_disorders IS NULL AND mhdx.other_mh IS NULL) AND (mhdx.focal_epilepsy IS NOT NULL OR mhdx.generalised_epilepsy IS NOT NULL OR mhdx.unclassified_epilepsy IS NOT NULL) THEN 'Eplilespy' 
		WHEN (mhdx.acute_transient_psychotic_disorder IS NOT NULL OR mhdx.acute_stress_reaction IS NOT NULL OR mhdx.adjustment_disorders IS NOT NULL OR mhdx.anxiety_disorder IS NOT NULL OR mhdx.bipolar_disorder IS NOT NULL OR mhdx.childhood_emotional_disorder IS NOT NULL OR mhdx.conduct_disorders IS NOT NULL OR mhdx.delirium IS NOT NULL OR mhdx.dementia IS NOT NULL OR mhdx.dissociative_conversion_disorder IS NOT NULL OR mhdx.dissociative_convulsions IS NOT NULL OR mhdx.hyperkinetic_disorder IS NOT NULL OR mhdx.intellectual_disability IS NOT NULL OR mhdx.disorders_due_drug_psychoactive_substances IS NOT NULL OR mhdx.disorders_due_alcohol IS NOT NULL OR mhdx.mild_depressive_episode IS NOT NULL OR mhdx.moderate_depressive_episode IS NOT NULL OR mhdx.nonorganic_enuresis IS NOT NULL OR mhdx.obsessive_compulsive_disorder IS NOT NULL OR mhdx.panic_disorder IS NOT NULL OR mhdx.pervasive_developmental_disorder IS NOT NULL OR mhdx.postpartum_depression IS NOT NULL OR mhdx.postpartum_psychosis IS NOT NULL OR mhdx.ptsd IS NOT NULL OR mhdx.schizophrenia IS NOT NULL OR mhdx.severe_depressive_episode_with_psychotic_symptoms IS NOT NULL OR mhdx.severe_depressive_episode_without_psychotic_symptoms IS NOT NULL OR mhdx.somatoform_disorders IS NOT NULL OR mhdx.other_mh IS NOT NULL) AND (mhdx.focal_epilepsy IS NULL AND mhdx.generalised_epilepsy IS NULL AND mhdx.unclassified_epilepsy IS NULL) THEN 'Mental Health' 
		WHEN (mhdx.acute_transient_psychotic_disorder IS NOT NULL OR mhdx.acute_stress_reaction IS NOT NULL OR mhdx.adjustment_disorders IS NOT NULL OR mhdx.anxiety_disorder IS NOT NULL OR mhdx.bipolar_disorder IS NOT NULL OR mhdx.childhood_emotional_disorder IS NOT NULL OR mhdx.conduct_disorders IS NOT NULL OR mhdx.delirium IS NOT NULL OR mhdx.dementia IS NOT NULL OR mhdx.dissociative_conversion_disorder IS NOT NULL OR mhdx.dissociative_convulsions IS NOT NULL OR mhdx.hyperkinetic_disorder IS NOT NULL OR mhdx.intellectual_disability IS NOT NULL OR mhdx.disorders_due_drug_psychoactive_substances IS NOT NULL OR mhdx.disorders_due_alcohol IS NOT NULL OR mhdx.mild_depressive_episode IS NOT NULL OR mhdx.moderate_depressive_episode IS NOT NULL OR mhdx.nonorganic_enuresis IS NOT NULL OR mhdx.obsessive_compulsive_disorder IS NOT NULL OR mhdx.panic_disorder IS NOT NULL OR mhdx.pervasive_developmental_disorder IS NOT NULL OR mhdx.postpartum_depression IS NOT NULL OR mhdx.postpartum_psychosis IS NOT NULL OR mhdx.ptsd IS NOT NULL OR mhdx.schizophrenia IS NOT NULL OR mhdx.severe_depressive_episode_with_psychotic_symptoms IS NOT NULL OR mhdx.severe_depressive_episode_without_psychotic_symptoms IS NOT NULL OR mhdx.somatoform_disorders IS NOT NULL OR mhdx.other_mh IS NOT NULL) AND (mhdx.focal_epilepsy IS NOT NULL OR mhdx.generalised_epilepsy IS NOT NULL OR mhdx.unclassified_epilepsy IS NOT NULL) THEN 'Mental Health + Eplilespy' 
		ELSE NULL 
	END AS cohort,
	cc.date::date AS visit_date,
	cc.visit_location,
	cc.intervention_setting,
	cc.type_of_activity,
	cc.visit_type,
	cc.provider_type,
	cc.encounter_id
FROM consultations_cte cc
LEFT OUTER JOIN cohort c
	ON cc.patient_id = c.patient_id AND cc.date >= c.intake_date AND (cc.date <= c.discharge_date OR c.discharge_date is NULL)
LEFT OUTER JOIN patient_identifier pi
	ON cc.patient_id = pi.patient_id
LEFT OUTER JOIN person_details_default pdd 
	ON cc.patient_id = pdd.person_id
LEFT OUTER JOIN person_address_default pad
	ON c.patient_id = pad.person_id
LEFT OUTER JOIN last_mh_diagnosis mhdx
	ON c.intake_encounter_id = mhdx.intake_encounter_id;