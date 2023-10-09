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
		ON ic1.patient_id = mhd.patient_id AND mhd.discharge_date > ic1.intake_date AND (mhd.discharge_date < ic2.intake_date OR ic2.intake_date IS NULL)),
-- The first psy initial assessment table extracts the date from the psy initial assessment as the cohort entry date. If multiple initial assessments are completed then the first is used.
first_psy_initial_assessment AS (
	SELECT DISTINCT ON (eec.patient_id, eec.entry_encounter_id, eec.intake_date, eec.discharge_date) eec.entry_encounter_id, pcia.date::date
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
-- The Syndrome sub-tables pivot syndrome data horizontally from the MH counselor initial assessment form. If more than one form is filled per cohort entry period than the data from the last form is reported. 
syndrome_pivot_cte AS (
	SELECT 
		DISTINCT ON (pcia.patient_id, pcia.date) pcia.patient_id, 
		pcia.date::date,
		MAX (CASE WHEN pcia.main_syndrome = 'Depression' OR pcia.additional_syndrome = 'Depression'  THEN 1 ELSE NULL END) AS depression,	
		MAX (CASE WHEN pcia.main_syndrome = 'Anxiety disorder' OR pcia.additional_syndrome = 'Anxiety disorder'  THEN 1 ELSE NULL END) AS anxiety_disorder,
		MAX (CASE WHEN pcia.main_syndrome = 'Trauma related symptoms' OR pcia.additional_syndrome = 'Trauma related symptoms'  THEN 1 ELSE NULL END) AS trauma_related_symptoms,	
		MAX (CASE WHEN pcia.main_syndrome = 'Adult behavioral / substance problem' OR pcia.additional_syndrome = 'Adult behavioral / substance problem'  THEN 1 ELSE NULL END) AS adult_behavioral_substance_problem,	
		MAX (CASE WHEN pcia.main_syndrome = 'Child behavioral problem' OR pcia.additional_syndrome = 'Child behavioral problem'  THEN 1 ELSE NULL END) AS child_behavioral_problem,	
		MAX (CASE WHEN pcia.main_syndrome = 'Psychosis' OR pcia.additional_syndrome = 'Psychosis'  THEN 1 ELSE NULL END) AS psychosis,	
		MAX (CASE WHEN pcia.main_syndrome = 'Psychosomatic problems' OR pcia.additional_syndrome = 'Psychosomatic problems'  THEN 1 ELSE NULL END) AS psychosomatic_problems,	
		MAX (CASE WHEN pcia.main_syndrome = 'Neurocognitive problem' OR pcia.additional_syndrome = 'Neurocognitive problem'  THEN 1 ELSE NULL END) AS neurocognitive_problem,	
		MAX (CASE WHEN pcia.main_syndrome = 'Epilepsy' OR pcia.additional_syndrome = 'Epilepsy'  THEN 1 ELSE NULL END) AS epilepsy,	
		MAX (CASE WHEN pcia.main_syndrome = 'Other' OR pcia.additional_syndrome = 'Other'  THEN 1 ELSE NULL END) AS other_syndrome
	FROM psy_counselors_initial_assessment pcia
	GROUP BY pcia.patient_id, pcia.date),
last_syndrome_cte AS (
	SELECT 
		DISTINCT ON (eec.patient_id, eec.entry_encounter_id) eec.patient_id,
		eec.entry_encounter_id,
		eec.intake_date, 
		eec.discharge_encounter_id,
		eec.discharge_date, 
		spc.date,
		spc.depression,	
		spc.anxiety_disorder,
		spc.trauma_related_symptoms,	
		spc.adult_behavioral_substance_problem,	
		spc.child_behavioral_problem,	
		spc.psychosis,	
		spc.psychosomatic_problems,	
		spc.neurocognitive_problem,	
		spc.epilepsy,	
		spc.other_syndrome
	FROM entry_exit_cte eec
	LEFT OUTER JOIN syndrome_pivot_cte spc
		ON eec.patient_id = spc.patient_id AND eec.intake_date <= spc.date AND CASE WHEN eec.discharge_date IS NOT NULL THEN eec.discharge_date ELSE current_date END >= spc.date
	GROUP BY eec.patient_id, eec.entry_encounter_id, eec.intake_date, eec.discharge_encounter_id, eec.discharge_date, spc.date, spc.depression, spc.anxiety_disorder, spc.trauma_related_symptoms, spc.adult_behavioral_substance_problem, spc.child_behavioral_problem, spc.psychosis, spc.psychosomatic_problems, spc.neurocognitive_problem, spc.epilepsy, spc.other_syndrome
	ORDER BY eec.patient_id, eec.entry_encounter_id, eec.intake_date, spc.date DESC),
-- The initial CGI-Severity score sub-table reports only one initial CGI-Severity score per cohort entry. If the score has been reported in mhGAP initial assessment form, then the most recent record is reported. If no mhGAP initial assessment form is completed, then the most recent record from the counselor initial assessment form is reported. 
initial_cgis_cte AS (
	SELECT
		DISTINCT ON (eec.patient_id, eec.entry_encounter_id, eec.intake_date, eec.discharge_date) eec.entry_encounter_id,
		CASE 
			WHEN pmia.cgi_s_score IS NOT NULL THEN pmia.cgi_s_score
			WHEN pmia.cgi_s_score IS NULL AND pcia.cgi_s_score IS NOT NULL THEN pcia.cgi_s_score
			ELSE NULL 
		END AS cgi_s_score_at_initial_assessment	
	FROM entry_exit_cte eec
	LEFT OUTER JOIN psychiatrist_mhgap_initial_assessment pmia 
		ON eec.patient_id = pmia.patient_id
	LEFT OUTER JOIN psy_counselors_initial_assessment pcia
		ON eec.patient_id = pcia.patient_id
	WHERE (pmia.date::date >= eec.intake_date AND (pmia.date::date <= eec.discharge_date OR eec.discharge_date IS NULL)) OR (pcia.date::date >= eec.intake_date AND (pcia.date::date <= eec.discharge_date OR eec.discharge_date IS NULL))
	ORDER BY eec.patient_id, eec.entry_encounter_id, eec.intake_date, eec.discharge_date, pmia.date::date DESC, pcia.date::date DESC),
-- The Mental Health diagnosis sub-tables pivot mental health diagnosis data horizontally from the Psychiatrist mhGap initial and follow-up forms. Only the last diagnoses reported are present. 
last_mh_main_dx_cte AS (
	SELECT 
		DISTINCT ON (eec.patient_id, eec.entry_encounter_id) eec.patient_id,
		eec.entry_encounter_id,
		eec.intake_date, 
		eec.discharge_encounter_id,
		eec.discharge_date,
		mmhd.date,
		mmhd.main_diagnosis AS diagnosis
	FROM entry_exit_cte eec
	LEFT OUTER JOIN (
		SELECT patient_id, date::date, main_diagnosis FROM psychiatrist_mhgap_initial_assessment
		UNION
		SELECT patient_id, date::date, main_diagnosis FROM psychiatrist_mhgap_follow_up) mmhd
		ON eec.patient_id = mmhd.patient_id AND eec.intake_date <= mmhd.date::date AND CASE WHEN eec.discharge_date IS NOT NULL THEN eec.discharge_date ELSE current_date END >= mmhd.date::date
	WHERE mmhd.main_diagnosis IS NOT NULL 
	GROUP BY eec.patient_id, eec.entry_encounter_id, eec.intake_date, eec.discharge_encounter_id, eec.discharge_date, mmhd.date::date,	mmhd.main_diagnosis 
	ORDER BY eec.patient_id, eec.entry_encounter_id, eec.intake_date, mmhd.date::date DESC),
last_mh_sec_dx_cte AS (
	SELECT 
		DISTINCT ON (eec.patient_id, eec.entry_encounter_id) eec.patient_id,
		eec.entry_encounter_id,
		eec.intake_date, 
		eec.discharge_encounter_id,
		eec.discharge_date,
		mmhd.date,
		mmhd.secondary_diagnosis AS diagnosis
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
		MAX (CASE WHEN mhdu.diagnosis = 'Obsessive compulsive disorder' THEN 1 ELSE NULL END) AS obsessive_compulsive_disorder,
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
-- The counselor initial assessment sub-table counts the number of initial counselor assessments that took place per cohort entry. 
counselor_ia_cte AS (
	SELECT 
		DISTINCT ON (eec.patient_id, eec.entry_encounter_id, eec.intake_date, eec.discharge_date) eec.entry_encounter_id,
		COUNT(*) AS counselor_initial_consultations
	FROM entry_exit_cte eec
	LEFT OUTER JOIN psy_counselors_initial_assessment pcia 
		ON eec.patient_id = pcia.patient_id
	WHERE pcia.date::date >= eec.intake_date AND (pcia.date::date <= eec.discharge_date OR eec.discharge_date IS NULL)
	GROUP BY eec.patient_id, eec.entry_encounter_id, eec.intake_date, eec.discharge_date),
-- The counselor follow-up sub-table counts the number of individual follow up counselor sessions that took place per cohort entry. 
counselor_fu_individual_cte AS (
	SELECT 
		DISTINCT ON (eec.patient_id, eec.entry_encounter_id, eec.intake_date, eec.discharge_date) eec.entry_encounter_id,
		COUNT(*) AS counselor_fu_individual_sessions
	FROM entry_exit_cte eec
	LEFT OUTER JOIN psy_counselors_follow_up pcfu 
		ON eec.patient_id = pcfu.patient_id
	WHERE pcfu.date::date >= eec.intake_date AND (pcfu.date::date <= eec.discharge_date OR eec.discharge_date IS NULL) AND pcfu.type_of_activity = 'Individual session'
	GROUP BY eec.patient_id, eec.entry_encounter_id, eec.intake_date, eec.discharge_date),
-- The counselor follow-up sub-table counts the number of other follow up counselor sessions that took place per cohort entry, excludes individual and missed sessions. 
counselor_fu_other_cte AS (
	SELECT 
		DISTINCT ON (eec.patient_id, eec.entry_encounter_id, eec.intake_date, eec.discharge_date) eec.entry_encounter_id,
		COUNT(*) AS counselor_fu_other_sessions
	FROM entry_exit_cte eec
	LEFT OUTER JOIN psy_counselors_follow_up pcfu 
		ON eec.patient_id = pcfu.patient_id
	WHERE pcfu.date::date >= eec.intake_date AND (pcfu.date::date <= eec.discharge_date OR eec.discharge_date IS NULL) AND (pcfu.type_of_activity != 'Individual session' OR pcfu.type_of_activity != 'Missed appointment')
	GROUP BY eec.patient_id, eec.entry_encounter_id, eec.intake_date, eec.discharge_date),
-- The psychiatrist initial assessment sub-table counts the number of initial counselor assessments that took place per cohort entry. 
psy_ia_cte AS (
	SELECT 
		DISTINCT ON (eec.patient_id, eec.entry_encounter_id, eec.intake_date, eec.discharge_date) eec.entry_encounter_id,
		COUNT(*) AS psychiatrist_initial_consultations
	FROM entry_exit_cte eec
	LEFT OUTER JOIN psychiatrist_mhgap_initial_assessment pmia 
		ON eec.patient_id = pmia.patient_id
	WHERE pmia.date::date >= eec.intake_date AND (pmia.date::date <= eec.discharge_date OR eec.discharge_date IS NULL)
	GROUP BY eec.patient_id, eec.entry_encounter_id, eec.intake_date, eec.discharge_date),
-- The psychiatrist follow-up sub-table counts the number of individual follow up psychiatrist sessions that took place per cohort entry. 
psy_fu_individual_cte AS (
	SELECT 
		DISTINCT ON (eec.patient_id, eec.entry_encounter_id, eec.intake_date, eec.discharge_date) eec.entry_encounter_id,
		COUNT(*) AS psychiatrist_fu_individual_sessions
	FROM entry_exit_cte eec
	LEFT OUTER JOIN psychiatrist_mhgap_follow_up pmfu 
		ON eec.patient_id = pmfu.patient_id
	WHERE pmfu.date::date >= eec.intake_date AND (pmfu.date::date <= eec.discharge_date OR eec.discharge_date IS NULL) AND pmfu.type_of_activity = 'Individual session'
	GROUP BY eec.patient_id, eec.entry_encounter_id, eec.intake_date, eec.discharge_date),
-- The psychiatrist follow-up sub-table counts the number of other follow up psychiatrist sessions that took place per cohort entry, excludes individual and missed sessions. 
psy_fu_other_cte AS (
	SELECT 
		DISTINCT ON (eec.patient_id, eec.entry_encounter_id, eec.intake_date, eec.discharge_date) eec.entry_encounter_id,
		COUNT(*) AS psychiatrist_fu_other_sessions
	FROM entry_exit_cte eec
	LEFT OUTER JOIN psychiatrist_mhgap_follow_up pmfu 
		ON eec.patient_id = pmfu.patient_id
	WHERE pmfu.date::date >= eec.intake_date AND (pmfu.date::date <= eec.discharge_date OR eec.discharge_date IS NULL) AND (pmfu.type_of_activity != 'Individual session' OR pmfu.type_of_activity != 'Missed appointment')
	GROUP BY eec.patient_id, eec.entry_encounter_id, eec.intake_date, eec.discharge_date),
-- The psychotropic prescription sub-table identifies any patient who has had at least one psychotropic prescription while in the cohort. Both past and current prescriptions are considered.
psychotropic_prescription_cte AS (
SELECT
	DISTINCT ON (eec.patient_id, eec.entry_encounter_id, eec.intake_date, eec.discharge_date) eec.entry_encounter_id,
	CASE 
		WHEN mdd.patient_id IS NOT NULL THEN 'Yes' 
		ELSE 'No'
	END AS psychotropic_prescription
FROM entry_exit_cte eec
LEFT OUTER JOIN medication_data_default mdd
	ON eec.patient_id = mdd.patient_id
WHERE mdd.start_date >= eec.intake_date AND (mdd.start_date <= eec.discharge_date OR eec.discharge_date IS NULL) AND mdd.coded_drug_name IS NOT NULL AND mdd.coded_drug_name != 'IBUPROFEN, 400 mg, tab.' AND mdd.coded_drug_name != 'PARACETAMOL (acetaminophen), 500 mg, tab.'),
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
	ORDER BY eec.patient_id, eec.entry_encounter_id, eec.intake_date, eec.discharge_date, vlc.date DESC)
-- Main query --
SELECT
	pi."Patient_Identifier",
	eec.patient_id,
	eec.entry_encounter_id,
	pa."Patient_code",
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
	pad.state_province AS governorate, 
	pad.city_village AS community_village,
	pad.address2 AS area,
	pa."Legal_status",
	pa."Civil_status",
	pa."Education_level",
	pa."Occupation",
	pa."Personal_Situation",
	pa."Living_conditions",
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
	CASE 
		WHEN lvlc.visit_location IS NOT NULL THEN lvlc.visit_location
		WHEN lvlc.visit_location IS NULL THEN mhi.visit_location 
		ELSE NULL 
	END AS last_visit_location,
	mhi.source_of_initial_patient_referral,
	CASE WHEN mhi.stressor_1 = 'Non-conflict-related medical condition' THEN 1 WHEN mhi.stressor_2 = 'Non-conflict-related medical condition' THEN 1 WHEN mhi.stressor_3 = 'Non-conflict-related medical condition' THEN 1 ELSE NULL END AS "stressor: non-conflict-related medical condition",
	CASE WHEN mhi.stressor_1 = 'Conflict-related medical condition' THEN 1 WHEN mhi.stressor_2 = 'Conflict-related medical condition' THEN 1 WHEN mhi.stressor_3 = 'Conflict-related medical condition' THEN 1 ELSE NULL END AS "stressor: conflict-related medical condition",
	CASE WHEN mhi.stressor_1 = 'Pre-existing mental health disorder' THEN 1 WHEN mhi.stressor_2 = 'Pre-existing mental health disorder' THEN 1 WHEN mhi.stressor_3 = 'Pre-existing mental health disorder' THEN 1 ELSE NULL END AS "stressor: pre-existing mental health disorder",
	CASE WHEN mhi.stressor_1 = 'Extreme poverty / Financial crisis' THEN 1 WHEN mhi.stressor_2 = 'Extreme poverty / Financial crisis' THEN 1 WHEN mhi.stressor_3 = 'Extreme poverty / Financial crisis' THEN 1 ELSE NULL END AS "stressor: extreme poverty / financial crisis",
	CASE WHEN mhi.stressor_1 = 'Hard living due to conflict' THEN 1 WHEN mhi.stressor_2 = 'Hard living due to conflict' THEN 1 WHEN mhi.stressor_3 = 'Hard living due to conflict' THEN 1 ELSE NULL END AS "stressor: hard living due to conflict",
	CASE WHEN mhi.stressor_1 = 'House / property destroyed' THEN 1 WHEN mhi.stressor_2 = 'House / property destroyed' THEN 1 WHEN mhi.stressor_3 = 'House / property destroyed' THEN 1 ELSE NULL END AS "stressor: house/ property destroyed",
	CASE WHEN mhi.stressor_1 = 'Intra-family related problem' THEN 1 WHEN mhi.stressor_2 = 'Intra-family related problem' THEN 1 WHEN mhi.stressor_3 = 'Intra-family related problem' THEN 1 ELSE NULL END AS "stressor: intra-family related problem",
	CASE WHEN mhi.stressor_1 = 'Close relative detained / died / missing / injured' THEN 1 WHEN mhi.stressor_2 = 'Close relative detained / died / missing / injured' THEN 1 WHEN mhi.stressor_3 = 'Close relative detained / died / missing / injured' THEN 1 ELSE NULL END AS "stressor: close relative detained/died/missing/injured",
	CASE WHEN mhi.stressor_1 = 'Close relative with medical disease' THEN 1 WHEN mhi.stressor_2 = 'Close relative with medical disease' THEN 1 WHEN mhi.stressor_3 = 'Close relative with medical disease' THEN 1 ELSE NULL END AS "stressor: close relative with medical disease",
	CASE WHEN mhi.stressor_1 = 'Loss or excessive social role' THEN 1 WHEN mhi.stressor_2 = 'Loss or excessive social role' THEN 1 WHEN mhi.stressor_3 = 'Loss or excessive social role' THEN 1 ELSE NULL END AS "stressor: loss or excessive social role",
	CASE WHEN mhi.stressor_1 = 'Victim of neglect' THEN 1 WHEN mhi.stressor_2 = 'Victim of neglect' THEN 1 WHEN mhi.stressor_3 = 'Victim of neglect' THEN 1 ELSE NULL END AS "stressor: victim of neglect",
	CASE WHEN mhi.stressor_1 = 'Isolated / Social exclusion' THEN 1 WHEN mhi.stressor_2 = 'Isolated / Social exclusion' THEN 1 WHEN mhi.stressor_3 = 'Isolated / Social exclusion' THEN 1 ELSE NULL END AS "stressor: isolated / social exclusion",
	CASE WHEN mhi.stressor_1 = 'Direct witness of violence' THEN 1 WHEN mhi.stressor_2 = 'Direct witness of violence' THEN 1 WHEN mhi.stressor_3 = 'Direct witness of violence' THEN 1 ELSE NULL END AS "stressor: direct witness of violence",
	CASE WHEN mhi.stressor_1 = 'Direct victim of violence' THEN 1 WHEN mhi.stressor_2 = 'Direct victim of violence' THEN 1 WHEN mhi.stressor_3 = 'Direct victim of violence' THEN 1 ELSE NULL END AS "stressor: direct victim of violence",
	CASE WHEN mhi.stressor_1 = 'Survivor of sexual violence' THEN 1 WHEN mhi.stressor_2 = 'Survivor of sexual violence' THEN 1 WHEN mhi.stressor_3 = 'Survivor of sexual violence' THEN 1 ELSE NULL END AS "stressor: survivor of sexual violence",
	CASE WHEN mhi.stressor_1 = 'Detained' THEN 1 WHEN mhi.stressor_2 = 'Detained' THEN 1 WHEN mhi.stressor_3 = 'Detained' THEN 1 ELSE NULL END AS "stressor: detained",
	CASE WHEN mhi.stressor_1 = 'Displaced due to conflict' THEN 1 WHEN mhi.stressor_2 = 'Displaced due to conflict' THEN 1 WHEN mhi.stressor_3 = 'Displaced due to conflict' THEN 1 ELSE NULL END AS "stressor: displaced due to conflict",
	CASE WHEN mhi.stressor_1 = 'Living in targeted village' THEN 1 WHEN mhi.stressor_2 = 'Living in targeted village' THEN 1 WHEN mhi.stressor_3 = 'Living in targeted village' THEN 1 ELSE NULL END AS "stressor: living in targeted village",
	CASE WHEN mhi.stressor_1 = 'Domestic violence' THEN 1 WHEN mhi.stressor_2 = 'Domestic violence' THEN 1 WHEN mhi.stressor_3 = 'Domestic violence' THEN 1 ELSE NULL END AS "stressor: domestic violence",
	CASE WHEN mhi.stressor_1 = 'None' THEN 1 WHEN mhi.stressor_2 = 'None' THEN 1 WHEN mhi.stressor_3 = 'None' THEN 1 ELSE NULL END AS "stressor: none",
	CASE WHEN mhi.stressor_1 IS NOT NULL AND mhi.stressor_1 != 'None' THEN 1 ELSE 0 END + CASE WHEN mhi.stressor_2 IS NOT NULL AND mhi.stressor_2 != 'None' THEN 1 ELSE 0 END + CASE WHEN mhi.stressor_3 IS NOT NULL AND mhi.stressor_3 != 'None' THEN 1 ELSE 0 END AS stressor_count,
	mhi.risk_factor_present,
	lsc.depression AS "syndrome: depression",	
	lsc.anxiety_disorder AS "syndrome: anxiety disorder",
	lsc.trauma_related_symptoms AS "syndrome: trauma related symptoms",	
	lsc.adult_behavioral_substance_problem AS "syndrome: adult behavioral substance problem",	
	lsc.child_behavioral_problem AS "syndrome: child behavioral problem",	
	lsc.psychosis AS "syndrome: psychosis",	
	lsc.psychosomatic_problems AS "syndrome: psychosomatic problems",	
	lsc.neurocognitive_problem AS "syndrome: neurocognitive problem",	
	lsc.epilepsy AS "syndrome: epilepsy",	
	lsc.other_syndrome AS "syndrome: other",
	icc.cgi_s_score_at_initial_assessment,
	mhdc.acute_transient_psychotic_disorder AS "diagnosis: acute and transient psychotic disorder",	
	mhdc.acute_stress_reaction AS "diagnosis: acute stress reaction",	
	mhdc.adjustment_disorders AS "diagnosis: adjustment disorders",	
	mhdc.anxiety_disorder AS "diagnosis: anxiety disorder",
	mhdc.bipolar_disorder AS "diagnosis: bipolar disorder",	
	mhdc.childhood_emotional_disorder AS "diagnosis: childhood emotional disorder",
	mhdc.conduct_disorders AS "diagnosis: conduct disorders",
	mhdc.delirium AS "diagnosis: delirium",	
	mhdc.dementia AS "diagnosis: dementia",	
	mhdc.dissociative_conversion_disorder AS "diagnosis: dissociative and conversion disorder",
	mhdc.dissociative_convulsions AS "diagnosis: dissociative convulsions",
	mhdc.hyperkinetic_disorder AS "diagnosis: hyperkinetic disorder",
	mhdc.intellectual_disability AS "diagnosis: intellectual disability",
	mhdc.disorders_due_drug_psychoactive_substances AS "diagnosis: mental/behavioural disorders due to drug/psychoactive substances use",
	mhdc.disorders_due_alcohol AS "diagnosis: mental or behavioural disorders due to use of alcohol",
	mhdc.mild_depressive_episode AS "diagnosis: mild depressive episode",
	mhdc.moderate_depressive_episode AS "diagnosis: moderate depressive episode",
	mhdc.nonorganic_enuresis AS "diagnosis: nonorganic enuresis",
	mhdc.obsessive_compulsive_disorder AS "diagnosis: obsessive compulsive disorder",
	mhdc.panic_disorder AS "diagnosis: panic disorder",
	mhdc.pervasive_developmental_disorder AS "diagnosis: pervasive developmental disorder",
	mhdc.postpartum_depression AS "diagnosis: post-partum depression",
	mhdc.postpartum_psychosis AS "diagnosis: post-partum psychosis",
	mhdc.ptsd AS "diagnosis: post traumatic stress disorder",
	mhdc.schizophrenia AS "diagnosis: schizophrenia",
	mhdc.severe_depressive_episode_with_psychotic_symptoms AS "diagnosis: severe depressive episode with psychotic symptoms",
	mhdc.severe_depressive_episode_without_psychotic_symptoms AS "diagnosis: severe depressive episode without psychotic symptoms",
	mhdc.somatoform_disorders AS "diagnosis: somatoform disorders",
	mhdc.other_mh AS "diagnosis: other mental health diagnosis",
	mhd.visit_location AS discharge_visit_location,
	mhd.intervention_setting AS discharge_intervention_setting,
	mhd.type_of_activity AS discharge_type_of_activity,
	mhd.mhos_at_discharge,
	mhd.cgi_s_score_at_discharge,
	mhd.cgi_i_score_at_discharge,
	mhd.patient_outcome,
	cic.counselor_initial_consultations,
	cfic.counselor_fu_individual_sessions,
	COALESCE(cic.counselor_initial_consultations,0) + COALESCE(cfic.counselor_fu_individual_sessions,0) AS counselor_individual_sessions,
	cfoc.counselor_fu_other_sessions,
	COALESCE(cic.counselor_initial_consultations,0) + COALESCE(cfic.counselor_fu_individual_sessions,0) + COALESCE(cfoc.counselor_fu_other_sessions,0) AS counselor_sessions,
	pic.psychiatrist_initial_consultations,
	pfic.psychiatrist_fu_individual_sessions,
	COALESCE(pic.psychiatrist_initial_consultations,0) + COALESCE(pfic.psychiatrist_fu_individual_sessions,0) AS psychiatrist_individual_sessions,
	pfoc.psychiatrist_fu_other_sessions,
	COALESCE(pic.psychiatrist_initial_consultations,0) + COALESCE(pfic.psychiatrist_fu_individual_sessions,0) + COALESCE(pfoc.psychiatrist_fu_other_sessions,0) AS psychiatrist_sessions,
	ppc.psychotropic_prescription
FROM entry_exit_cte eec
LEFT OUTER JOIN patient_identifier pi
	ON eec.patient_id = pi.patient_id
LEFT OUTER JOIN person_attributes pa
	ON eec.patient_id = pa.person_id
LEFT OUTER JOIN person_address_default pad
	ON eec.patient_id = pad.person_id
LEFT OUTER JOIN person_details_default pdd 
	ON eec.patient_id = pdd.person_id
LEFT OUTER JOIN patient_encounter_details_default ped 
	ON eec.entry_encounter_id = ped.encounter_id
LEFT OUTER JOIN first_psy_initial_assessment fpia
	ON eec.entry_encounter_id = fpia.entry_encounter_id
LEFT OUTER JOIN first_clinician_initial_assessment fcia
	ON eec.entry_encounter_id = fcia.entry_encounter_id
LEFT OUTER JOIN mental_health_intake mhi
	ON eec.entry_encounter_id = mhi.encounter_id
LEFT OUTER JOIN last_syndrome_cte lsc 
	ON eec.entry_encounter_id = lsc.entry_encounter_id
LEFT OUTER JOIN initial_cgis_cte icc
	ON eec.entry_encounter_id = icc.entry_encounter_id
LEFT OUTER JOIN mental_health_discharge mhd 
	ON eec.discharge_encounter_id = mhd.encounter_id
LEFT OUTER JOIN last_mh_diagnosis_cte mhdc
	ON eec.entry_encounter_id = mhdc.entry_encounter_id 
LEFT OUTER JOIN counselor_ia_cte cic 
	ON eec.entry_encounter_id = cic.entry_encounter_id
LEFT OUTER JOIN counselor_fu_individual_cte cfic 
	ON eec.entry_encounter_id = cfic.entry_encounter_id
LEFT OUTER JOIN counselor_fu_other_cte cfoc 
	ON eec.entry_encounter_id = cfoc.entry_encounter_id
LEFT OUTER JOIN psy_ia_cte pic 
	ON eec.entry_encounter_id = pic.entry_encounter_id
LEFT OUTER JOIN psy_fu_individual_cte pfic 
	ON eec.entry_encounter_id = pfic.entry_encounter_id
LEFT OUTER JOIN psy_fu_other_cte pfoc 
	ON eec.entry_encounter_id = pfoc.entry_encounter_id
LEFT OUTER JOIN psychotropic_prescription_cte ppc
	ON eec.entry_encounter_id = ppc.entry_encounter_id
LEFT OUTER JOIN last_visit_location_cte lvlc
	ON eec.entry_encounter_id = lvlc.entry_encounter_id;
