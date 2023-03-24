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
entry_exit_cte AS (
	SELECT
		e1.patient_id, 
		e1.entry_encounter_id,
		e1.entry_date, 
		mhd.discharge_date,
		mhd.encounter_id AS discharge_encounter_id
	FROM entry1_cte e1
	LEFT OUTER JOIN entry2_cte e2
		ON e1.entry1_id = e2.entry2_id::int
	LEFT OUTER JOIN (SELECT patient_id, discharge_date, encounter_id FROM mental_health_discharge) mhd
		ON e1.patient_id = mhd.patient_id 
		AND mhd.discharge_date > e1.entry_date 
		AND (mhd.discharge_date < e2.entry_date OR e2.entry_date IS NULL)
	ORDER BY e1.patient_id, e2.entry_date),
ncd_diagnosis_cte AS (
		SELECT 
		DISTINCT ON (eec.patient_id, eec.entry_encounter_id, eec.entry_date, eec.discharge_date) eec.entry_encounter_id,
		CASE 
			WHEN ncdd.diagnosis = 'Focal epilepsy' THEN 1 ELSE NULL 
		END AS focal_epilepsy,
		CASE 
			WHEN ncdd.diagnosis = 'Generalised epilepsy' THEN 1 ELSE NULL 
		END AS generalised_epilepsy,
		CASE 
			WHEN ncdd.diagnosis = 'Unclassified epilepsy' THEN 1 ELSE NULL 
		END AS unclassified_epilepsy,
		CASE 
			WHEN ncdd.diagnosis = 'Other' THEN 1 ELSE NULL 
		END AS other
	FROM entry_exit_cte eec 
	LEFT OUTER JOIN (SELECT d.patient_id, d.diagnosis, n.date FROM diagnosis d LEFT OUTER JOIN ncd n ON d.encounter_id = n.encounter_id) ncdd
		ON eec.patient_id = ncdd.patient_id
	WHERE ncdd.date >= eec.entry_date AND (ncdd.date <= eec.discharge_date OR eec.discharge_date IS NULL)),
mh_diagnosis_cte AS (
	SELECT 
		DISTINCT ON (eec.patient_id, eec.entry_encounter_id, eec.entry_date, eec.discharge_date) eec.entry_encounter_id,
		CASE 
			WHEN pmia.main_diagnosis = 'Acute and transient psychotic disorder' OR
			pmia.secondary_diagnosis = 'Acute and transient psychotic disorder' OR 
			pmfu.main_diagnosis = 'Acute and transient psychotic disorder' OR
			pmfu.secondary_diagnosis = 'Acute and transient psychotic disorder' THEN 1 ELSE NULL 
		END AS acute_transient_psychotic_disorder,	
		CASE 
			WHEN pmia.main_diagnosis = 'Acute stress reaction' OR
			pmia.secondary_diagnosis = 'Acute stress reaction' OR 
			pmfu.main_diagnosis = 'Acute stress reaction' OR
			pmfu.secondary_diagnosis = 'Acute stress reaction' THEN 1 ELSE NULL 
		END AS acute_stress_reaction,	
		CASE 
			WHEN pmia.main_diagnosis = 'Adjustment disorders' OR
			pmia.secondary_diagnosis = 'Adjustment disorders' OR 
			pmfu.main_diagnosis = 'Adjustment disorders' OR
			pmfu.secondary_diagnosis = 'Adjustment disorders' THEN 1 ELSE NULL 
		END AS adjustment_disorders,	
		CASE 
			WHEN pmia.main_diagnosis = 'Anxiety disorder' OR
			pmia.secondary_diagnosis = 'Anxiety disorder' OR 
			pmfu.main_diagnosis = 'Anxiety disorder' OR
			pmfu.secondary_diagnosis = 'Anxiety disorder' THEN 1 ELSE NULL 
		END AS anxiety_disorder,
		CASE 
			WHEN pmia.main_diagnosis = 'Bipolar disorder' OR
			pmia.secondary_diagnosis = 'Bipolar disorder' OR 
			pmfu.main_diagnosis = 'Bipolar disorder' OR
			pmfu.secondary_diagnosis = 'Bipolar disorder' THEN 1 ELSE NULL 
		END AS bipolar_disorder,	
	 	CASE 
			WHEN pmia.main_diagnosis = 'Childhood emotional disorder' OR
			pmia.secondary_diagnosis = 'Childhood emotional disorder' OR 
			pmfu.main_diagnosis = 'Childhood emotional disorder' OR
			pmfu.secondary_diagnosis = 'Childhood emotional disorder' THEN 1 ELSE NULL 
		END AS childhood_emotional_disorder,
		CASE 
			WHEN pmia.main_diagnosis = 'Conduct disorders' OR
			pmia.secondary_diagnosis = 'Conduct disorders' OR 
			pmfu.main_diagnosis = 'Conduct disorders' OR
			pmfu.secondary_diagnosis = 'Conduct disorders' THEN 1 ELSE NULL 
		END AS conduct_disorders,
		CASE 
			WHEN pmia.main_diagnosis = 'Delirium' OR
			pmia.secondary_diagnosis = 'Delirium' OR 
			pmfu.main_diagnosis = 'Delirium' OR
			pmfu.secondary_diagnosis = 'Delirium' THEN 1 ELSE NULL 
		END AS delirium,	
		CASE 
			WHEN pmia.main_diagnosis = 'Dementia' OR
			pmia.secondary_diagnosis = 'Dementia' OR 
			pmfu.main_diagnosis = 'Dementia' OR
			pmfu.secondary_diagnosis = 'Dementia' THEN 1 ELSE NULL 
		END AS dementia,	
		CASE 
			WHEN pmia.main_diagnosis = 'Dissociative and conversion disorder' OR
			pmia.secondary_diagnosis = 'Dissociative and conversion disorder' OR 
			pmfu.main_diagnosis = 'Dissociative and conversion disorder' OR
			pmfu.secondary_diagnosis = 'Dissociative and conversion disorder' THEN 1 ELSE NULL 
		END AS dissociative_conversion_disorder,
		CASE 
			WHEN pmia.main_diagnosis = 'Dissociative convulsions' OR
			pmia.secondary_diagnosis = 'Dissociative convulsions' OR 
			pmfu.main_diagnosis = 'Dissociative convulsions' OR
			pmfu.secondary_diagnosis = 'Dissociative convulsions' THEN 1 ELSE NULL 
		END AS dissociative_convulsions,
		CASE 
			WHEN pmia.main_diagnosis = 'Hyperkinetic disorder' OR
			pmia.secondary_diagnosis = 'Hyperkinetic disorder' OR 
			pmfu.main_diagnosis = 'Hyperkinetic disorder' OR
			pmfu.secondary_diagnosis = 'Hyperkinetic disorder' THEN 1 ELSE NULL 
		END AS hyperkinetic_disorder,
		CASE 
			WHEN pmia.main_diagnosis = 'Intellectual disability' OR
			pmia.secondary_diagnosis = 'Intellectual disability' OR 
			pmfu.main_diagnosis = 'Intellectual disability' OR
			pmfu.secondary_diagnosis = 'Intellectual disability' THEN 1 ELSE NULL 
		END AS intellectual_disability,
		CASE 
			WHEN pmia.main_diagnosis = 'Mental or behavioural disorders due to multiple drug use or other psychoactive substances' OR
			pmia.secondary_diagnosis = 'Mental or behavioural disorders due to multiple drug use or other psychoactive substances' OR 
			pmfu.main_diagnosis = 'Mental or behavioural disorders due to multiple drug use or other psychoactive substances' OR
			pmfu.secondary_diagnosis = 'Mental or behavioural disorders due to multiple drug use or other psychoactive substances' THEN 1 ELSE NULL 
		END AS disorders_due_drug_psychoactive_substances,
		CASE 
			WHEN pmia.main_diagnosis = 'Mental or behavioural disorders due to use of alcohol' OR
			pmia.secondary_diagnosis = 'Mental or behavioural disorders due to use of alcohol' OR 
			pmfu.main_diagnosis = 'Mental or behavioural disorders due to use of alcohol' OR
			pmfu.secondary_diagnosis = 'Mental or behavioural disorders due to use of alcohol' THEN 1 ELSE NULL 
		END AS disorders_due_alcohol,
		CASE 
			WHEN pmia.main_diagnosis = 'Mild depressive episode' OR
			pmia.secondary_diagnosis = 'Mild depressive episode' OR 
			pmfu.main_diagnosis = 'Mild depressive episode' OR
			pmfu.secondary_diagnosis = 'Mild depressive episode' THEN 1 ELSE NULL 
		END AS mild_depressive_episode,
		CASE 
			WHEN pmia.main_diagnosis = 'Moderate depressive episode' OR
			pmia.secondary_diagnosis = 'Moderate depressive episode' OR 
			pmfu.main_diagnosis = 'Moderate depressive episode' OR
			pmfu.secondary_diagnosis = 'Moderate depressive episode' THEN 1 ELSE NULL 
		END AS moderate_depressive_episode,
		CASE 
			WHEN pmia.main_diagnosis = 'Nonorganic enuresis' OR
			pmia.secondary_diagnosis = 'Nonorganic enuresis' OR 
			pmfu.main_diagnosis = 'Nonorganic enuresis' OR
			pmfu.secondary_diagnosis = 'Nonorganic enuresis' THEN 1	ELSE NULL 
		END AS nonorganic_enuresis,
		CASE 
			WHEN pmia.main_diagnosis = 'Obsessive compulsive disorder' OR
			pmia.secondary_diagnosis = 'Obsessive compulsive disorder' OR 
			pmfu.main_diagnosis = 'Obsessive compulsive disorder' OR
			pmfu.secondary_diagnosis = 'Obsessive compulsive disorder' THEN 1 ELSE NULL 
		END AS obsessive_compulsive_disorder,
		CASE 
			WHEN pmia.main_diagnosis = 'Panic disorder' OR
			pmia.secondary_diagnosis = 'Panic disorder' OR 
			pmfu.main_diagnosis = 'Panic disorder' OR
			pmfu.secondary_diagnosis = 'Panic disorder' THEN 1 ELSE NULL 
		END AS panic_disorder,
		CASE 
			WHEN pmia.main_diagnosis = 'Pervasive developmental disorder' OR
			pmia.secondary_diagnosis = 'Pervasive developmental disorder' OR 
			pmfu.main_diagnosis = 'Pervasive developmental disorder' OR
			pmfu.secondary_diagnosis = 'Pervasive developmental disorder' THEN 1 ELSE NULL 
		END AS pervasive_developmental_disorder,
		CASE 
			WHEN pmia.main_diagnosis = 'Post-partum depression' OR
			pmia.secondary_diagnosis = 'Post-partum depression' OR 
			pmfu.main_diagnosis = 'Post-partum depression' OR
			pmfu.secondary_diagnosis = 'Post-partum depression' THEN 1 ELSE NULL 
		END AS postpartum_depression,
		CASE 
			WHEN pmia.main_diagnosis = 'Post-partum psychosis' OR
			pmia.secondary_diagnosis = 'Post-partum psychosis' OR 
			pmfu.main_diagnosis = 'Post-partum psychosis' OR
			pmfu.secondary_diagnosis = 'Post-partum psychosis' THEN 1 ELSE NULL 
		END AS postpartum_psychosis,
		CASE 
			WHEN pmia.main_diagnosis = 'Post Traumatic Stress Disorder' OR
			pmia.secondary_diagnosis = 'Post Traumatic Stress Disorder' OR 
			pmfu.main_diagnosis = 'Post Traumatic Stress Disorder' OR
			pmfu.secondary_diagnosis = 'Post Traumatic Stress Disorder' THEN 1 ELSE NULL 
		END AS ptsd,
		CASE 
			WHEN pmia.main_diagnosis = 'Schizophrenia' OR
			pmia.secondary_diagnosis = 'Schizophrenia' OR 
			pmfu.main_diagnosis = 'Schizophrenia' OR
			pmfu.secondary_diagnosis = 'Schizophrenia' THEN 1 ELSE NULL 
		END AS schizophrenia,
		CASE 
			WHEN pmia.main_diagnosis = 'Severe depressive episode with psychotic symptoms' OR
			pmia.secondary_diagnosis = 'Severe depressive episode with psychotic symptoms' OR 
			pmfu.main_diagnosis = 'Severe depressive episode with psychotic symptoms' OR
			pmfu.secondary_diagnosis = 'Severe depressive episode with psychotic symptoms' THEN 1 ELSE NULL 
		END AS severe_depressive_episode_with_psychotic_symptoms,
		CASE 
			WHEN pmia.main_diagnosis = 'Severe depressive episode without psychotic symptoms' OR
			pmia.secondary_diagnosis = 'Severe depressive episode without psychotic symptoms' OR 
			pmfu.main_diagnosis = 'Severe depressive episode without psychotic symptoms' OR
			pmfu.secondary_diagnosis = 'Severe depressive episode without psychotic symptoms' THEN 1 ELSE NULL 
		END AS severe_depressive_episode_without_psychotic_symptoms,
		CASE 
			WHEN pmia.main_diagnosis = 'Somatoform disorders' OR
			pmia.secondary_diagnosis = 'Somatoform disorders' OR 
			pmfu.main_diagnosis = 'Somatoform disorders' OR
			pmfu.secondary_diagnosis = 'Somatoform disorders' THEN 1 ELSE NULL 
		END AS somatoform_disorders,
		CASE 
			WHEN pmia.main_diagnosis = 'Other' OR
			pmia.secondary_diagnosis = 'Other' OR 
			pmfu.main_diagnosis = 'Other' OR
			pmfu.secondary_diagnosis = 'Other' THEN 1 ELSE NULL 
		END AS other
	FROM entry_exit_cte eec 
	LEFT OUTER JOIN (SELECT pmia2.* FROM psychiatrist_mhgap_initial_assessment pmia2 JOIN entry_exit_cte eec ON eec.patient_id = pmia2.patient_id WHERE pmia2.date >= eec.entry_date AND (pmia2.date <= eec.discharge_date OR eec.discharge_date IS NULL)) pmia
		ON eec.patient_id = pmia.patient_id
	LEFT OUTER JOIN (SELECT pmfu2.* FROM psychiatrist_mhgap_follow_up pmfu2 JOIN entry_exit_cte eec ON eec.patient_id = pmfu2.patient_id WHERE pmfu2.date >= eec.entry_date AND (pmfu2.date <= eec.discharge_date OR eec.discharge_date IS NULL)) pmfu
		ON eec.patient_id = pmfu.patient_id),
consultations_cte AS (
	SELECT
		pcia.date,
		pcia.patient_id,
		pcia.visit_location,
		pcia.intervention_setting,
		'Individual session' AS type_of_activity,
		'Initial' AS visit_type,
		'Counselor' AS provider_type
	FROM psy_counselors_initial_assessment pcia 
	UNION
	SELECT
		pmia.date,
		pmia.patient_id,
		pmia.visit_location,
		pmia.intervention_setting,
		'Individual session' AS type_of_activity,
		'Initial' AS visit_type,
		'Psychiatrist' AS provider_type
	FROM psychiatrist_mhgap_initial_assessment pmia
	UNION
	SELECT
		pcfu.date,
		pcfu.patient_id,
		pcfu.visit_location,
		pcfu.intervention_setting,
		pcfu.type_of_activity,
		'Follow up' AS visit_type,
		'Counselor' AS provider_type
	FROM psy_counselors_follow_up pcfu 
	UNION
	SELECT 
		pmfu.date,
		pmfu.patient_id,
		pmfu.visit_location,
		pmfu.intervention_setting,
		pmfu.type_of_activity,
		'Follow up' AS visit_type,
		'Psychiatrist' AS provider_type
	FROM psychiatrist_mhgap_follow_up pmfu)
SELECT 
	DISTINCT ON (cc.patient_id, cc.date, cc.visit_location, cc.intervention_setting, cc.type_of_activity, cc.visit_type, cc.provider_type) cc.patient_id,
	pi."Patient_Identifier",
	pdd.age AS age_current,
	CASE 
		WHEN pdd.age::int <= 3 THEN '0-3'
		WHEN pdd.age::int >= 4 AND pdd.age::int <= 7 THEN '04-07'
		WHEN pdd.age::int >= 8 AND pdd.age::int <= 14 THEN '08-14'
		WHEN pdd.age::int >= 15 AND pdd.age::int <= 17 THEN '15-17'
		WHEN pdd.age::int >= 18 THEN '18+'
		ELSE NULL
	END AS age_group_current,
	pdd.gender,
	CASE
		WHEN (ncddc.focal_epilepsy IS NOT NULL OR ncddc.generalised_epilepsy IS NOT NULL OR ncddc.unclassified_epilepsy IS NOT NULL OR ncddc.other IS NOT NULL) AND 
		mhdc.acute_transient_psychotic_disorder IS NULL AND mhdc.acute_stress_reaction IS NULL AND mhdc.adjustment_disorders IS NULL AND mhdc.anxiety_disorder IS NULL AND mhdc.bipolar_disorder IS NULL AND mhdc.childhood_emotional_disorder IS NULL AND mhdc.conduct_disorders IS NULL AND mhdc.delirium IS NULL AND mhdc.dementia IS NULL AND mhdc.dissociative_conversion_disorder IS NULL AND mhdc.dissociative_convulsions IS NULL AND mhdc.hyperkinetic_disorder IS NULL AND mhdc.intellectual_disability IS NULL AND mhdc.disorders_due_drug_psychoactive_substances IS NULL AND mhdc.disorders_due_alcohol IS NULL AND mhdc.mild_depressive_episode IS NULL AND mhdc.moderate_depressive_episode IS NULL AND mhdc.nonorganic_enuresis IS NULL AND mhdc.obsessive_compulsive_disorder IS NULL AND mhdc.panic_disorder IS NULL AND mhdc.pervasive_developmental_disorder IS NULL AND mhdc.postpartum_depression IS NULL AND mhdc.postpartum_psychosis IS NULL AND mhdc.ptsd IS NULL AND mhdc.schizophrenia IS NULL AND mhdc.severe_depressive_episode_with_psychotic_symptoms IS NULL AND mhdc.severe_depressive_episode_without_psychotic_symptoms IS NULL AND mhdc.somatoform_disorders IS NULL AND mhdc.other IS NULL THEN 'Epilepsy'
		WHEN (mhdc.acute_transient_psychotic_disorder IS NOT NULL OR mhdc.acute_stress_reaction IS NOT NULL OR mhdc.adjustment_disorders IS NOT NULL OR mhdc.anxiety_disorder IS NOT NULL OR mhdc.bipolar_disorder IS NOT NULL OR mhdc.childhood_emotional_disorder IS NOT NULL OR mhdc.conduct_disorders IS NOT NULL OR mhdc.delirium IS NOT NULL OR mhdc.dementia IS NOT NULL OR mhdc.dissociative_conversion_disorder IS NOT NULL OR mhdc.dissociative_convulsions IS NOT NULL OR mhdc.hyperkinetic_disorder IS NOT NULL OR mhdc.intellectual_disability IS NOT NULL OR mhdc.disorders_due_drug_psychoactive_substances IS NOT NULL OR mhdc.disorders_due_alcohol IS NOT NULL OR mhdc.mild_depressive_episode IS NOT NULL OR mhdc.moderate_depressive_episode IS NOT NULL OR mhdc.nonorganic_enuresis IS NOT NULL OR mhdc.obsessive_compulsive_disorder IS NOT NULL OR mhdc.panic_disorder IS NOT NULL OR mhdc.pervasive_developmental_disorder IS NOT NULL OR mhdc.postpartum_depression IS NOT NULL OR mhdc.postpartum_psychosis IS NOT NULL OR mhdc.ptsd IS NOT NULL OR mhdc.schizophrenia IS NOT NULL OR mhdc.severe_depressive_episode_with_psychotic_symptoms IS NOT NULL OR mhdc.severe_depressive_episode_without_psychotic_symptoms IS NOT NULL OR mhdc.somatoform_disorders IS NOT NULL OR mhdc.other IS NOT NULL) AND 
		ncddc.focal_epilepsy IS NULL AND ncddc.generalised_epilepsy IS NULL AND ncddc.unclassified_epilepsy IS NULL AND ncddc.other IS NULL THEN 'Mental health'
		WHEN (mhdc.acute_transient_psychotic_disorder IS NOT NULL OR mhdc.acute_stress_reaction IS NOT NULL OR mhdc.adjustment_disorders IS NOT NULL OR mhdc.anxiety_disorder IS NOT NULL OR mhdc.bipolar_disorder IS NOT NULL OR mhdc.childhood_emotional_disorder IS NOT NULL OR mhdc.conduct_disorders IS NOT NULL OR mhdc.delirium IS NOT NULL OR mhdc.dementia IS NOT NULL OR mhdc.dissociative_conversion_disorder IS NOT NULL OR mhdc.dissociative_convulsions IS NOT NULL OR mhdc.hyperkinetic_disorder IS NOT NULL OR mhdc.intellectual_disability IS NOT NULL OR mhdc.disorders_due_drug_psychoactive_substances IS NOT NULL OR mhdc.disorders_due_alcohol IS NOT NULL OR mhdc.mild_depressive_episode IS NOT NULL OR mhdc.moderate_depressive_episode IS NOT NULL OR mhdc.nonorganic_enuresis IS NOT NULL OR mhdc.obsessive_compulsive_disorder IS NOT NULL OR mhdc.panic_disorder IS NOT NULL OR mhdc.pervasive_developmental_disorder IS NOT NULL OR mhdc.postpartum_depression IS NOT NULL OR mhdc.postpartum_psychosis IS NOT NULL OR mhdc.ptsd IS NOT NULL OR mhdc.schizophrenia IS NOT NULL OR mhdc.severe_depressive_episode_with_psychotic_symptoms IS NOT NULL OR mhdc.severe_depressive_episode_without_psychotic_symptoms IS NOT NULL OR mhdc.somatoform_disorders IS NOT NULL OR mhdc.other IS NOT NULL) AND 
		(ncddc.focal_epilepsy IS NOT NULL OR ncddc.generalised_epilepsy IS NOT NULL OR ncddc.unclassified_epilepsy IS NOT NULL AND ncddc.other IS NOT NULL) THEN 'Mental health/epilepsy'
		ELSE NULL 
	END AS cohort,
	cc.date AS visit_date,
	cc.visit_location,
	cc.intervention_setting,
	cc.type_of_activity,
	cc.visit_type,
	cc.provider_type	
FROM consultations_cte cc
LEFT OUTER JOIN entry_exit_cte eec
	ON cc.patient_id = eec.patient_id AND cc.date >= eec.entry_date AND (cc.date <= eec.discharge_date OR eec.discharge_date is NULL)
LEFT OUTER JOIN patient_identifier pi
	ON cc.patient_id = pi.patient_id
LEFT OUTER JOIN person_details_default pdd 
	ON cc.patient_id = pdd.person_id
LEFT OUTER JOIN ncd_diagnosis_cte ncddc
	ON eec.entry_encounter_id = ncddc.entry_encounter_id 
LEFT OUTER JOIN mh_diagnosis_cte mhdc
	ON eec.entry_encounter_id = mhdc.entry_encounter_id 