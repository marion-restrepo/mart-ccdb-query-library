-- The first 3 sub-tables of the query build the frame for monitoring patient entry to and exit from the cohort/active treatment. The table is build by listing all entry dates as recorded in the MH intake form. For each exit date is matched to an intake following the logic that is occurs after the entry date but not before the next entry date, in the case that a patient enters the cohort more than once.
WITH entry_cte_2 AS (
	SELECT patient_id, encounter_id AS entry_encounter_id, date::date AS entry_date, CONCAT(patient_id, ROW_NUMBER () OVER (PARTITION BY patient_id ORDER BY date)) AS entry_id_2, 1 AS one
	FROM mental_health_intake),
entry_cte_1 AS (
	SELECT patient_id, entry_encounter_id, entry_date, entry_id_2::int+one AS entry_id_1
	FROM entry_cte_2),
entry_exit_cte AS (
	SELECT ec1.patient_id, ec1.entry_encounter_id, ec1.entry_date, CASE WHEN ec2.entry_date IS NOT NULL THEN 1 ELSE null END as readmission, mhd.discharge_date::date, mhd.encounter_id AS discharge_encounter_id
	FROM entry_cte_1 ec1
	LEFT OUTER JOIN entry_cte_2 ec2
		ON ec1.entry_id_1::int = ec2.entry_id_2::int
	LEFT OUTER JOIN (SELECT patient_id, discharge_date, encounter_id FROM mental_health_discharge) mhd
		ON ec1.patient_id = mhd.patient_id AND mhd.discharge_date >= ec1.entry_date AND (mhd.discharge_date < ec2.entry_date OR ec2.entry_date IS NULL)),
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
		eec.entry_date, 
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
		ON eec.patient_id = spc.patient_id AND eec.entry_date <= spc.date AND CASE WHEN eec.discharge_date IS NOT NULL THEN eec.discharge_date ELSE current_date END >= spc.date
	GROUP BY eec.patient_id, eec.entry_encounter_id, eec.entry_date, eec.discharge_encounter_id, eec.discharge_date, spc.date, spc.depression, spc.anxiety_disorder, spc.trauma_related_symptoms, spc.adult_behavioral_substance_problem, spc.child_behavioral_problem, spc.psychosis, spc.psychosomatic_problems, spc.neurocognitive_problem, spc.epilepsy, spc.other_syndrome
	ORDER BY eec.patient_id, eec.entry_encounter_id, eec.entry_date, spc.date DESC),
-- The initial CGI-Severity score sub-table reports only one initial CGI-Severity score per cohort entry. If the score has been reported in mhGAP initial assessment form, then the most recent record is reported. If no mhGAP initial assessment form is completed, then the most recent record from the counselor initial assessment form is reported. 
initial_cgis_cte AS (
	SELECT
		DISTINCT ON (eec.patient_id, eec.entry_encounter_id, eec.entry_date, eec.discharge_date) eec.entry_encounter_id,
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
	WHERE pmia.date::date >= eec.entry_date AND (pmia.date::date <= eec.discharge_date OR eec.discharge_date IS NULL) AND pcia.date::date >= eec.entry_date AND (pcia.date::date <= eec.discharge_date OR eec.discharge_date IS NULL)
	ORDER BY eec.patient_id, eec.entry_encounter_id, eec.entry_date, eec.discharge_date, pmia.date::date DESC, pcia.date::date DESC),
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
-- The NCD form sub-table extracts the last NCD form per cohort entry to look at if there are values reported for pregnancy, hospitalization, missed medication, or seizures since the last visit. 
last_ncd_form_cte AS (
SELECT 
		DISTINCT ON (eec.patient_id, eec.entry_encounter_id) eec.patient_id,
		eec.entry_encounter_id,
		eec.entry_date, 
		eec.discharge_encounter_id,
		eec.discharge_date, 
		n.date::date AS last_ncd_date,
		n.visit_type,
		n.currently_pregnant AS pregnant_last_visit,
		n.hospitalised_since_last_visit AS hospitalised_last_visit,
		n.missed_medication_doses_in_last_7_days AS missed_medication_last_visit,
		n.seizures_since_last_visit AS seizures_last_visit
	FROM entry_exit_cte eec
	LEFT OUTER JOIN ncd n
		ON eec.patient_id = n.patient_id AND eec.entry_date <= n.date::date AND CASE WHEN eec.discharge_date IS NOT NULL THEN eec.discharge_date ELSE current_date END >= n.date::date
	ORDER BY eec.patient_id, eec.entry_encounter_id, n.patient_id, n.date::date DESC),		
-- The counselor initial assessment sub-table counts the number of initial counselor assessments that took place per cohort entry. 
counselor_ia_cte AS (
	SELECT 
		DISTINCT ON (eec.patient_id, eec.entry_encounter_id, eec.entry_date, eec.discharge_date) eec.entry_encounter_id,
		COUNT(*) AS counselor_initial_consultations
	FROM entry_exit_cte eec
	LEFT OUTER JOIN psy_counselors_initial_assessment pcia 
		ON eec.patient_id = pcia.patient_id
	WHERE pcia.date::date >= eec.entry_date AND (pcia.date::date <= eec.discharge_date OR eec.discharge_date IS NULL)
	GROUP BY eec.patient_id, eec.entry_encounter_id, eec.entry_date, eec.discharge_date),
-- The counselor follow-up sub-table counts the number of individual follow up counselor sessions that took place per cohort entry. 
counselor_fu_individual_cte AS (
	SELECT 
		DISTINCT ON (eec.patient_id, eec.entry_encounter_id, eec.entry_date, eec.discharge_date) eec.entry_encounter_id,
		COUNT(*) AS counselor_fu_individual_sessions
	FROM entry_exit_cte eec
	LEFT OUTER JOIN psy_counselors_follow_up pcfu 
		ON eec.patient_id = pcfu.patient_id
	WHERE pcfu.date::date >= eec.entry_date AND (pcfu.date::date <= eec.discharge_date OR eec.discharge_date IS NULL) AND pcfu.type_of_activity = 'Individual session'
	GROUP BY eec.patient_id, eec.entry_encounter_id, eec.entry_date, eec.discharge_date),
-- The counselor follow-up sub-table counts the number of other follow up counselor sessions that took place per cohort entry, excludes individual and missed sessions. 
counselor_fu_other_cte AS (
	SELECT 
		DISTINCT ON (eec.patient_id, eec.entry_encounter_id, eec.entry_date, eec.discharge_date) eec.entry_encounter_id,
		COUNT(*) AS counselor_fu_other_sessions
	FROM entry_exit_cte eec
	LEFT OUTER JOIN psy_counselors_follow_up pcfu 
		ON eec.patient_id = pcfu.patient_id
	WHERE pcfu.date::date >= eec.entry_date AND (pcfu.date::date <= eec.discharge_date OR eec.discharge_date IS NULL) AND (pcfu.type_of_activity != 'Individual session' OR pcfu.type_of_activity != 'Missed appointment')
	GROUP BY eec.patient_id, eec.entry_encounter_id, eec.entry_date, eec.discharge_date),
-- The psychiatrist initial assessment sub-table counts the number of initial counselor assessments that took place per cohort entry. 
psy_ia_cte AS (
	SELECT 
		DISTINCT ON (eec.patient_id, eec.entry_encounter_id, eec.entry_date, eec.discharge_date) eec.entry_encounter_id,
		COUNT(*) AS psychiatrist_initial_consultations
	FROM entry_exit_cte eec
	LEFT OUTER JOIN psychiatrist_mhgap_initial_assessment pmia 
		ON eec.patient_id = pmia.patient_id
	WHERE pmia.date::date >= eec.entry_date AND (pmia.date::date <= eec.discharge_date OR eec.discharge_date IS NULL)
	GROUP BY eec.patient_id, eec.entry_encounter_id, eec.entry_date, eec.discharge_date),
-- The psychiatrist follow-up sub-table counts the number of individual follow up psychiatrist sessions that took place per cohort entry. 
psy_fu_individual_cte AS (
	SELECT 
		DISTINCT ON (eec.patient_id, eec.entry_encounter_id, eec.entry_date, eec.discharge_date) eec.entry_encounter_id,
		COUNT(*) AS psychiatrist_fu_individual_sessions
	FROM entry_exit_cte eec
	LEFT OUTER JOIN psychiatrist_mhgap_follow_up pmfu 
		ON eec.patient_id = pmfu.patient_id
	WHERE pmfu.date::date >= eec.entry_date AND (pmfu.date::date <= eec.discharge_date OR eec.discharge_date IS NULL) AND pmfu.type_of_activity = 'Individual session'
	GROUP BY eec.patient_id, eec.entry_encounter_id, eec.entry_date, eec.discharge_date),
-- The psychiatrist follow-up sub-table counts the number of other follow up psychiatrist sessions that took place per cohort entry, excludes individual and missed sessions. 
psy_fu_other_cte AS (
	SELECT 
		DISTINCT ON (eec.patient_id, eec.entry_encounter_id, eec.entry_date, eec.discharge_date) eec.entry_encounter_id,
		COUNT(*) AS psychiatrist_fu_other_sessions
	FROM entry_exit_cte eec
	LEFT OUTER JOIN psychiatrist_mhgap_follow_up pmfu 
		ON eec.patient_id = pmfu.patient_id
	WHERE pmfu.date::date >= eec.entry_date AND (pmfu.date::date <= eec.discharge_date OR eec.discharge_date IS NULL) AND (pmfu.type_of_activity != 'Individual session' OR pmfu.type_of_activity != 'Missed appointment')
	GROUP BY eec.patient_id, eec.entry_encounter_id, eec.entry_date, eec.discharge_date),
-- The NCD sub-table counts the number of NCD consultations that took place per cohort entry. Includes initial, follow-up, and discharge consultations. Excludes discharge consultations where the outcome has been recorded as 'lost to follow up' or 'deceased'.
ncd_cte AS (
	SELECT 
		DISTINCT ON (eec.patient_id, eec.entry_encounter_id, eec.entry_date, eec.discharge_date) eec.entry_encounter_id,
		COUNT(*) AS ncd_consultations
	FROM entry_exit_cte eec
	LEFT OUTER JOIN ncd n  
		ON eec.patient_id = n.patient_id
	WHERE n.date::date >= eec.entry_date AND (n.date::date <= eec.discharge_date OR eec.discharge_date IS NULL) AND (n.patient_outcome IS NULL OR n.patient_outcome != 'Lost to follow up' OR n.patient_outcome != 'Deceased')
	GROUP BY eec.patient_id, eec.entry_encounter_id, eec.entry_date, eec.discharge_date),
-- The psychotropic prescription sub-table identifies any patient who has had at least one psychotropic prescription while in the cohort. Both past and current prescriptions are considered.
psychotropic_prescription_cte AS (
SELECT
	DISTINCT ON (eec.patient_id, eec.entry_encounter_id, eec.entry_date, eec.discharge_date) eec.entry_encounter_id,
	CASE 
		WHEN mdd.patient_id IS NOT NULL THEN 'Yes' 
		ELSE 'No'
	END AS psychotropic_prescription
FROM entry_exit_cte eec
LEFT OUTER JOIN medication_data_default mdd
	ON eec.patient_id = mdd.patient_id
WHERE mdd.start_date >= eec.entry_date AND (mdd.start_date <= eec.discharge_date OR eec.discharge_date IS NULL) AND mdd.coded_drug_name IS NOT NULL AND mdd.coded_drug_name != 'FOLIC acid, 5 mg, tab.'),
-- The visit location sub-table finds the last visit location reported across all clinical consultaiton/session forms.
last_visit_location_cte AS (	
	SELECT 
		DISTINCT ON (eec.patient_id, eec.entry_encounter_id, eec.entry_date, eec.discharge_date) eec.entry_encounter_id,
		vlc.visit_location AS visit_location
	FROM entry_exit_cte eec
	LEFT OUTER JOIN (
		SELECT n.date::date, n.patient_id, n.visit_location FROM ncd n WHERE n.visit_location IS NOT NULL 
		UNION
		SELECT pcia.date::date, pcia.patient_id, pcia.visit_location FROM psy_counselors_initial_assessment pcia WHERE pcia.visit_location IS NOT NULL 
		UNION 
		SELECT pmia.date::date, pmia.patient_id, pmia.visit_location FROM psychiatrist_mhgap_initial_assessment pmia WHERE pmia.visit_location IS NOT NULL 
		UNION
		SELECT pcfu.date::date, pcfu.patient_id, pcfu.visit_location FROM psy_counselors_follow_up pcfu WHERE pcfu.visit_location IS NOT NULL 
		UNION
		SELECT pmfu.date::date, pmfu.patient_id, pmfu.visit_location FROM psychiatrist_mhgap_follow_up pmfu WHERE pmfu.visit_location IS NOT NULL
		UNION
		SELECT mhd.discharge_date AS date, mhd.patient_id, mhd.location AS visit_location FROM mental_health_discharge mhd WHERE mhd.location IS NOT NULL) vlc
		ON eec.patient_id = vlc.patient_id
	WHERE vlc.date >= eec.entry_date AND (vlc.date <= eec.discharge_date OR eec.discharge_date IS NULL)
	GROUP BY eec.patient_id, eec.entry_encounter_id, eec.entry_date, eec.discharge_date, vlc.date, vlc.visit_location
	ORDER BY eec.patient_id, eec.entry_encounter_id, eec.entry_date, eec.discharge_date, vlc.date DESC)
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
	pa."patientState", 
	pa."Education_level",
	pa."Personal_Situation",
	pa."Living_conditions",
	CASE
		WHEN (ncddc.focal_epilepsy IS NOT NULL OR ncddc.generalised_epilepsy IS NOT NULL OR ncddc.unclassified_epilepsy IS NOT NULL OR ncddc.other_ncd IS NOT NULL) AND 
		mhdc.acute_transient_psychotic_disorder IS NULL AND mhdc.acute_stress_reaction IS NULL AND mhdc.adjustment_disorders IS NULL AND mhdc.anxiety_disorder IS NULL AND mhdc.bipolar_disorder IS NULL AND mhdc.childhood_emotional_disorder IS NULL AND mhdc.conduct_disorders IS NULL AND mhdc.delirium IS NULL AND mhdc.dementia IS NULL AND mhdc.dissociative_conversion_disorder IS NULL AND mhdc.dissociative_convulsions IS NULL AND mhdc.hyperkinetic_disorder IS NULL AND mhdc.intellectual_disability IS NULL AND mhdc.disorders_due_drug_psychoactive_substances IS NULL AND mhdc.disorders_due_alcohol IS NULL AND mhdc.mild_depressive_episode IS NULL AND mhdc.moderate_depressive_episode IS NULL AND mhdc.nonorganic_enuresis IS NULL AND mhdc.obsessive_compulsive_disorder IS NULL AND mhdc.panic_disorder IS NULL AND mhdc.pervasive_developmental_disorder IS NULL AND mhdc.postpartum_depression IS NULL AND mhdc.postpartum_psychosis IS NULL AND mhdc.ptsd IS NULL AND mhdc.schizophrenia IS NULL AND mhdc.severe_depressive_episode_with_psychotic_symptoms IS NULL AND mhdc.severe_depressive_episode_without_psychotic_symptoms IS NULL AND mhdc.somatoform_disorders IS NULL AND mhdc.other_mh IS NULL THEN 'Epilepsy'
		WHEN (mhdc.acute_transient_psychotic_disorder IS NOT NULL OR mhdc.acute_stress_reaction IS NOT NULL OR mhdc.adjustment_disorders IS NOT NULL OR mhdc.anxiety_disorder IS NOT NULL OR mhdc.bipolar_disorder IS NOT NULL OR mhdc.childhood_emotional_disorder IS NOT NULL OR mhdc.conduct_disorders IS NOT NULL OR mhdc.delirium IS NOT NULL OR mhdc.dementia IS NOT NULL OR mhdc.dissociative_conversion_disorder IS NOT NULL OR mhdc.dissociative_convulsions IS NOT NULL OR mhdc.hyperkinetic_disorder IS NOT NULL OR mhdc.intellectual_disability IS NOT NULL OR mhdc.disorders_due_drug_psychoactive_substances IS NOT NULL OR mhdc.disorders_due_alcohol IS NOT NULL OR mhdc.mild_depressive_episode IS NOT NULL OR mhdc.moderate_depressive_episode IS NOT NULL OR mhdc.nonorganic_enuresis IS NOT NULL OR mhdc.obsessive_compulsive_disorder IS NOT NULL OR mhdc.panic_disorder IS NOT NULL OR mhdc.pervasive_developmental_disorder IS NOT NULL OR mhdc.postpartum_depression IS NOT NULL OR mhdc.postpartum_psychosis IS NOT NULL OR mhdc.ptsd IS NOT NULL OR mhdc.schizophrenia IS NOT NULL OR mhdc.severe_depressive_episode_with_psychotic_symptoms IS NOT NULL OR mhdc.severe_depressive_episode_without_psychotic_symptoms IS NOT NULL OR mhdc.somatoform_disorders IS NOT NULL OR mhdc.other_mh IS NOT NULL) AND 
		ncddc.focal_epilepsy IS NULL AND ncddc.generalised_epilepsy IS NULL AND ncddc.unclassified_epilepsy IS NULL AND ncddc.other_ncd IS NULL THEN 'Mental health'
		WHEN (mhdc.acute_transient_psychotic_disorder IS NOT NULL OR mhdc.acute_stress_reaction IS NOT NULL OR mhdc.adjustment_disorders IS NOT NULL OR mhdc.anxiety_disorder IS NOT NULL OR mhdc.bipolar_disorder IS NOT NULL OR mhdc.childhood_emotional_disorder IS NOT NULL OR mhdc.conduct_disorders IS NOT NULL OR mhdc.delirium IS NOT NULL OR mhdc.dementia IS NOT NULL OR mhdc.dissociative_conversion_disorder IS NOT NULL OR mhdc.dissociative_convulsions IS NOT NULL OR mhdc.hyperkinetic_disorder IS NOT NULL OR mhdc.intellectual_disability IS NOT NULL OR mhdc.disorders_due_drug_psychoactive_substances IS NOT NULL OR mhdc.disorders_due_alcohol IS NOT NULL OR mhdc.mild_depressive_episode IS NOT NULL OR mhdc.moderate_depressive_episode IS NOT NULL OR mhdc.nonorganic_enuresis IS NOT NULL OR mhdc.obsessive_compulsive_disorder IS NOT NULL OR mhdc.panic_disorder IS NOT NULL OR mhdc.pervasive_developmental_disorder IS NOT NULL OR mhdc.postpartum_depression IS NOT NULL OR mhdc.postpartum_psychosis IS NOT NULL OR mhdc.ptsd IS NOT NULL OR mhdc.schizophrenia IS NOT NULL OR mhdc.severe_depressive_episode_with_psychotic_symptoms IS NOT NULL OR mhdc.severe_depressive_episode_without_psychotic_symptoms IS NOT NULL OR mhdc.somatoform_disorders IS NOT NULL OR mhdc.other_mh IS NOT NULL) AND 
		(ncddc.focal_epilepsy IS NOT NULL OR ncddc.generalised_epilepsy IS NOT NULL OR ncddc.unclassified_epilepsy IS NOT NULL AND ncddc.other_ncd IS NOT NULL) THEN 'Mental health/epilepsy'
		ELSE NULL 
	END AS cohort,
	eec.entry_date, 
	eec.discharge_date,
	CASE 
		WHEN eec.discharge_date IS NULL THEN 'Yes'
		ELSE null
	END AS active,
	eec.readmission,
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
	CASE WHEN mhi.stressor_1 = 'Distress as a result of Ebola outbreak' THEN 1 WHEN mhi.stressor_2 = 'Distress as a result of Ebola outbreak' THEN 1 WHEN mhi.stressor_3 = 'Distress as a result of Ebola outbreak' THEN 1 ELSE NULL END AS "stressor: distress as a result of ebola outbreak",
	CASE WHEN mhi.stressor_1 = 'Stigma' THEN 1 WHEN mhi.stressor_2 = 'Stigma' THEN 1 WHEN mhi.stressor_3 = 'Stigma' THEN 1 ELSE NULL END AS "stressor: stigma",
	CASE WHEN mhi.stressor_1 = 'Domestic violence' THEN 1 WHEN mhi.stressor_2 = 'Domestic violence' THEN 1 WHEN mhi.stressor_3 = 'Domestic violence' THEN 1 ELSE NULL END AS "stressor: domestic violence",
	CASE WHEN mhi.stressor_1 = 'None' THEN 1 WHEN mhi.stressor_2 = 'None' THEN 1 WHEN mhi.stressor_3 = 'None' THEN 1 ELSE NULL END AS "stressor: none",
	CASE WHEN mhi.stressor_1 IS NOT NULL AND mhi.stressor_1 != 'None' THEN 1 ELSE 0 END +
		CASE WHEN mhi.stressor_2 IS NOT NULL AND mhi.stressor_2 != 'None' THEN 1 ELSE 0 END +
		CASE WHEN mhi.stressor_3 IS NOT NULL AND mhi.stressor_3 != 'None' THEN 1 ELSE 0 END AS stressor_count,
	mhi.risk_factor_present,
	lsc.depression AS "synrome: depression",	
	lsc.anxiety_disorder AS "synrome: anxiety disorder",
	lsc.trauma_related_symptoms AS "synrome: trauma related symptoms",	
	lsc.adult_behavioral_substance_problem AS "synrome: adult behavioral substance problem",	
	lsc.child_behavioral_problem AS "synrome: child behavioral problem",	
	lsc.psychosis AS "synrome: psychosis",	
	lsc.psychosomatic_problems AS "synrome: psychosomatic problems",	
	lsc.neurocognitive_problem AS "synrome: neurocognitive problem",	
	lsc.epilepsy AS "synrome: epilepsy",	
	lsc.other_syndrome AS "synrome: other",
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
	ncddc.focal_epilepsy AS "diagnosis: focal epilepsy",
	ncddc.generalised_epilepsy AS "diagnosis: generalised epilepsy",
	ncddc.unclassified_epilepsy AS "diagnosis: unclassified epilepsy",
	ncddc.other_ncd AS "diagnosis: other epilepsy",
	lnfc.last_ncd_date,
	lnfc.pregnant_last_visit,
	lnfc.hospitalised_last_visit,
	lnfc.missed_medication_last_visit,
	lnfc.seizures_last_visit,
	mhd.location AS discharge_visit_location,
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
	nc.ncd_consultations,
	ppc.psychotropic_prescription
FROM entry_exit_cte eec
LEFT OUTER JOIN patient_identifier pi
	ON eec.patient_id = pi.patient_id
LEFT OUTER JOIN person_attributes pa
	ON eec.patient_id = pa.person_id
LEFT OUTER JOIN person_details_default pdd 
	ON eec.patient_id = pdd.person_id
LEFT OUTER JOIN patient_encounter_details_default ped 
	ON eec.entry_encounter_id = ped.encounter_id
LEFT OUTER JOIN mental_health_intake mhi
	ON eec.entry_encounter_id = mhi.encounter_id
LEFT OUTER JOIN last_syndrome_cte lsc 
	ON eec.entry_encounter_id = lsc.entry_encounter_id
LEFT OUTER JOIN initial_cgis_cte icc
	ON eec.entry_encounter_id = icc.entry_encounter_id
LEFT OUTER JOIN mental_health_discharge mhd 
	ON eec.discharge_encounter_id = mhd.encounter_id
LEFT OUTER JOIN last_ncd_diagnosis_cte ncddc
	ON eec.entry_encounter_id = ncddc.entry_encounter_id 
LEFT OUTER JOIN last_mh_diagnosis_cte mhdc
	ON eec.entry_encounter_id = mhdc.entry_encounter_id 
LEFT OUTER JOIN last_ncd_form_cte lnfc 
	ON eec.entry_encounter_id = lnfc.entry_encounter_id 
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
LEFT OUTER JOIN ncd_cte nc
	ON eec.entry_encounter_id = nc.entry_encounter_id
LEFT OUTER JOIN psychotropic_prescription_cte ppc
	ON eec.entry_encounter_id = ppc.entry_encounter_id
LEFT OUTER JOIN last_visit_location_cte lvlc
	ON eec.entry_encounter_id = lvlc.entry_encounter_id;