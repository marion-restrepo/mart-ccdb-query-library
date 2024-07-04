-- The first CTE build the frame for patients entering and exiting the cohort. This frame is based on MNT VIH TB form with visit types of 'Inclusion' and 'Sortie'. For each patient, the query takes all initial visit dates and matches the discharge visit date occuring after the initial visit. If a patient has multiple initial visits and discharge visits, the match happens sequentially based on the date of visit (e.g. the first initial visit is matched to the first discharge, and so on). If the patient does not have a discharge visit, then the discharge information is empty until completed. 
WITH inclusion AS (
	SELECT 
		patient_id, encounter_id AS encounter_id_inclusion, lieu_de_visite AS lieu_de_visite_inclusion, date AS date_inclusion, DENSE_RANK () OVER (PARTITION BY patient_id ORDER BY date) AS inclusion_visit_order, LEAD (date) OVER (PARTITION BY patient_id ORDER BY date) AS date_inclusion_suivi
	FROM mnt_vih_tb WHERE type_de_visite = 'Inclusion'),
cohorte AS (
	SELECT
		i.patient_id, i.encounter_id_inclusion, i.lieu_de_visite_inclusion, i.date_inclusion, CASE WHEN i.inclusion_visit_order > 1 THEN 'Oui' END readmission, d.encounter_id AS encounter_id_sortie, CASE WHEN d.date_de_sortie IS NOT NULL THEN d.date_de_sortie WHEN d.date_de_sortie IS NULL THEN d.date ELSE NULL END AS date_de_sortie, d.statut_de_sortie AS statut_de_sortie
	FROM inclusion i
	LEFT JOIN (SELECT patient_id, date, encounter_id, date_de_sortie, statut_de_sortie FROM mnt_vih_tb WHERE type_de_visite = 'Sortie') d 
		ON i.patient_id = d.patient_id AND d.date >= i.date_inclusion AND (d.date < i.date_inclusion_suivi OR i.date_inclusion_suivi IS NULL)),
-- The PTPE CTE looks at if the patient has a PTPE form completed. 
dernière_ptpe AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.encounter_id_inclusion, c.date_inclusion, c.date_de_sortie) c.encounter_id_inclusion,
		ptpe.date AS date_derniere_ptpe
	FROM cohorte c
	LEFT OUTER JOIN ptpe
		ON c.patient_id = ptpe.patient_id AND c.date_inclusion <= ptpe.date::date AND CASE WHEN c.date_de_sortie IS NOT NULL THEN c.date_de_sortie ELSE current_date END >= ptpe.date::date
	GROUP BY c.patient_id, c.encounter_id_inclusion, c.date_inclusion, c.date_de_sortie, ptpe.date
	ORDER BY c.patient_id, c.encounter_id_inclusion, c.date_inclusion, c.date_de_sortie, ptpe.date DESC),
-- The last completed form CTE looks at the last date and type of visit for each patient based on the clinical forms (including MNT VIH TB, PTPE).
dernière_fiche AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.encounter_id_inclusion, c.date_inclusion, c.date_de_sortie) c.encounter_id_inclusion,
		forms.date AS date_derniere_visite,
		forms.dernière_fiche_type,
		CASE WHEN forms.form_field_path = 'NCD2' THEN 'MNT/VIH/TB' WHEN forms.form_field_path = 'PMTCT' THEN 'PTPE' ELSE NULL END AS type_derniere_fiche
	FROM cohorte c
	LEFT OUTER JOIN (SELECT patient_id, CASE WHEN type_de_visite = 'Sortie' AND date_de_sortie IS NOT NULL THEN date_de_sortie 
	ELSE date END AS date, type_de_visite AS dernière_fiche_type, form_field_path FROM mnt_vih_tb UNION SELECT patient_id, date, type_de_visite AS dernière_fiche_type, form_field_path FROM ptpe) forms
		ON c.patient_id = forms.patient_id AND c.date_inclusion <= forms.date::date AND CASE WHEN c.date_de_sortie IS NOT NULL THEN c.date_de_sortie ELSE current_date END >= forms.date::date
	GROUP BY c.patient_id, c.encounter_id_inclusion, c.date_inclusion, c.date_de_sortie, forms.date, forms.dernière_fiche_type, forms.form_field_path
	ORDER BY c.patient_id, c.encounter_id_inclusion, c.date_inclusion, c.date_de_sortie, forms.date DESC, forms.form_field_path),
-- The last visit location CTE finds the last visit location reported in clinical forms (including MNT VIH TB, PTPE).
dernière_fiche_location AS (	
	SELECT 
		DISTINCT ON (c.patient_id, c.encounter_id_inclusion, c.date_inclusion, c.date_de_sortie) c.encounter_id_inclusion,
		forms.lieu_de_visite AS dernière_fiche_location
	FROM cohorte c
	LEFT OUTER JOIN (SELECT patient_id, CASE WHEN type_de_visite = 'Sortie' AND date_de_sortie IS NOT NULL THEN date_de_sortie ELSE date END AS date, lieu_de_visite FROM mnt_vih_tb UNION 
	SELECT patient_id, date, lieu_de_visite FROM ptpe) forms
		ON c.patient_id = forms.patient_id AND c.date_inclusion <= forms.date::date AND CASE WHEN c.date_de_sortie IS NOT NULL THEN c.date_de_sortie ELSE current_date END >= forms.date::date
	WHERE forms.lieu_de_visite IS NOT NULL
	GROUP BY c.patient_id, c.encounter_id_inclusion, c.date_inclusion, c.date_de_sortie, forms.date, forms.lieu_de_visite
	ORDER BY c.patient_id, c.encounter_id_inclusion, c.date_inclusion, c.date_de_sortie, forms.date DESC),
-- The diagnosis CTE selects all reported diagnoses per cohort enrollment, both listing and pivoting the data horizonally. The pivoted diagnosis data is presented with the date the diagnosis was first reported.
diagnostic_cohorte AS (
	SELECT
		DISTINCT ON (d.patient_id, d.diagnostic) d.patient_id, c.encounter_id_inclusion, n.date, d.diagnostic
	FROM diagnostic d 
	LEFT JOIN mnt_vih_tb n USING(encounter_id)
	LEFT JOIN cohorte c ON d.patient_id = c.patient_id AND c.date_inclusion <= n.date AND CASE WHEN c.date_de_sortie IS NOT NULL THEN c.date_de_sortie ELSE current_date END >= n.date
	ORDER BY d.patient_id, d.diagnostic, n.date),
diagnostic_cohorte_pivot AS (
	SELECT 
		DISTINCT ON (encounter_id_inclusion, patient_id) encounter_id_inclusion, 
		patient_id,
		MAX (CASE WHEN diagnostic = 'Asthme' THEN date::date ELSE NULL END) AS asthme,
		MAX (CASE WHEN diagnostic = 'Drépanocytose' THEN date::date ELSE NULL END) AS drépanocytose,
		MAX (CASE WHEN diagnostic = 'Insuffisance renale chronique' THEN date::date ELSE NULL END) AS insuffisance_renal_chronique,
		MAX (CASE WHEN diagnostic = 'Maladie cardiovasculaire' THEN date::date ELSE NULL END) AS maladie_cardiovasculaire,
		MAX (CASE WHEN diagnostic = 'Bronchopneumopathie chronique obstructive' THEN date::date ELSE NULL END) AS bronchopneumopathie_chronique_obstructive,
		MAX (CASE WHEN diagnostic = 'Diabète sucré de type 1' THEN date::date ELSE NULL END) AS diabète_type1,
		MAX (CASE WHEN diagnostic = 'Diabète sucré de type 2' THEN date::date ELSE NULL END) AS diabète_type2,
		MAX (CASE WHEN diagnostic = 'Hypertension' THEN date::date ELSE NULL END) AS hypertension,
		MAX (CASE WHEN diagnostic = 'Hypothyroïdie' THEN date::date ELSE NULL END) AS hypothyroïdie,
		MAX (CASE WHEN diagnostic = 'Hyperthyroïdie' THEN date::date ELSE NULL END) AS hyperthyroïdie,
		MAX (CASE WHEN diagnostic = 'Épilepsie focale' THEN date::date ELSE NULL END) AS épilepsie_focale,
		MAX (CASE WHEN diagnostic = 'Épilepsie généralisée' THEN date::date ELSE NULL END) AS épilepsie_généralisée,
		MAX (CASE WHEN diagnostic = 'Épilepsie non classifiée' THEN date::date ELSE NULL END) AS épilepsie_non_classifiée,
		MAX (CASE WHEN diagnostic = 'Tuberculose pulmonaire' THEN date::date ELSE NULL END) AS tb_pulmonaire,
		MAX (CASE WHEN diagnostic = 'Tuberculose extrapulmonaire' THEN date::date ELSE NULL END) AS tb_extrapulmonaire,
		MAX (CASE WHEN diagnostic = 'Infection par le VIH' THEN date::date ELSE NULL END) AS vih,
		MAX (CASE WHEN diagnostic = 'Troubles de santé mentale' THEN date::date ELSE NULL END) AS troubles_de_santé_mentale,
		MAX (CASE WHEN diagnostic = 'Autre' THEN date::date ELSE NULL END) AS autre_diagnostic,
		MAX (CASE WHEN diagnostic IN ('Asthme','Drépanocytose','Insuffisance renale chronique','Maladie cardiovasculaire','Bronchopneumopathie chronique obstructive','Diabète sucré de type 1','Diabète sucré de type 2','Hypertension','Hypothyroïdie','Hyperthyroïdie','Épilepsie focale','Épilepsie généralisée','Épilepsie non classifiée','Autre') THEN 'Oui' ELSE NULL END) AS mnt,
		MAX (CASE WHEN diagnostic IN ('Tuberculose pulmonaire','Tuberculose extrapulmonaire') THEN 'Oui' ELSE NULL END) AS tb		
	FROM diagnostic_cohorte
	GROUP BY encounter_id_inclusion, patient_id),
diagnostic_cohorte_liste AS (
	SELECT encounter_id_inclusion, STRING_AGG(diagnostic, ', ') AS liste_diagnostic
	FROM diagnostic_cohorte
	GROUP BY encounter_id_inclusion),
-- The risk factor CTE pivots the risk factor data horizontally from the MNT VIH TB form. Only the last risk factors are reported per cohort enrollment are present. 
facteurs_risque_cohorte AS (
	SELECT
		DISTINCT ON (fr.patient_id, fr.facteurs_de_risque) fr.patient_id, c.encounter_id_inclusion, n.date, fr.facteurs_de_risque
	FROM facteurs_de_risque fr
	LEFT JOIN mnt_vih_tb n USING(encounter_id)  
	LEFT JOIN cohorte c ON fr.patient_id = c.patient_id AND c.date_inclusion <= n.date AND CASE WHEN c.date_de_sortie IS NOT NULL THEN c.date_de_sortie ELSE current_date END >= n.date
	ORDER BY fr.patient_id, fr.facteurs_de_risque, n.date),
facteurs_risque_pivot AS (
	SELECT 
		DISTINCT ON (encounter_id_inclusion, patient_id) encounter_id_inclusion, 
		patient_id, 
		MAX (CASE WHEN facteurs_de_risque = 'Traditional medicine' THEN 'Oui' ELSE NULL END) AS médecine_traditionnelle,
		MAX (CASE WHEN facteurs_de_risque = 'Second-hand smoking' THEN 'Oui' ELSE NULL END) AS tabagisme_passif,
		MAX (CASE WHEN facteurs_de_risque = 'Smoker' THEN 'Oui' ELSE NULL END) AS fumeur,
		MAX (CASE WHEN facteurs_de_risque = 'Alcohol use' THEN 'Oui' ELSE NULL END) AS consommation_alcool,
		MAX (CASE WHEN facteurs_de_risque = 'Other' THEN 'Oui' ELSE NULL END) AS autre_facteurs_risque
	FROM facteurs_risque_cohorte
	GROUP BY encounter_id_inclusion, patient_id),
-- The ARV initiation CTE provides the ARV initiation date reported in the MNT VIH TB form. The firt date of ARV initiation is reported. 
instauration_arv AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.encounter_id_inclusion, c.date_inclusion, c.date_de_sortie) c.encounter_id_inclusion,
		mvt.date_d_instauration_des_arv AS date_instauration_arv
	FROM cohorte c
	LEFT OUTER JOIN mnt_vih_tb mvt
		ON c.patient_id = mvt.patient_id AND c.date_inclusion <= mvt.date::date AND CASE WHEN c.date_de_sortie IS NOT NULL THEN c.date_de_sortie ELSE current_date END >= mvt.date::date
	WHERE mvt.date_d_instauration_des_arv IS NOT NULL
	GROUP BY c.patient_id, c.encounter_id_inclusion, c.date_inclusion, c.date_de_sortie, mvt.date, mvt.date_d_instauration_des_arv
	ORDER BY c.patient_id, c.encounter_id_inclusion, c.date_inclusion, c.date_de_sortie, mvt.date ASC),
-- The ARV medication CTE 
médicament_arv AS (
	SELECT
		DISTINCT ON (c.patient_id, c.encounter_id_inclusion, c.date_inclusion, c.date_de_sortie) c.encounter_id_inclusion,
		CASE WHEN mdd.patient_id IS NOT NULL THEN 'Oui' ELSE NULL END AS traitement_arv_actuellement
	FROM cohorte c 
	LEFT OUTER JOIN medication_data_default mdd
		ON c.patient_id = mdd.patient_id AND c.date_inclusion <= mdd.start_date::date AND CASE WHEN c.date_de_sortie IS NOT NULL THEN c.date_de_sortie ELSE current_date END >= mdd.start_date::date
	WHERE mdd.coded_drug_name IN ('ABC 120 mg / 3TC 60 mg, disp. tab.','ABC 600 mg / 3TC 300 mg, tab.','ATV 300 mg / r 100 mg, tab.','AZT 60 mg / 3TC 30 mg , disp. tab.','DARUNAVIR ethanolate (DRV), eq. 600 mg base, tab.','DOLUTEGRAVIR sodium (DTG), eq. 10mg base, disp. tab.','DOLUTEGRAVIR sodium (DTG), eq. 50 mg base, tab.','DORALPVR1P- LPV 40 mg / r 10 mg, granules dans gélule','LPV 200mg / r 50mg, tab.','TDF 300 mg / FTC 200 mg / DTG 50 mg, tab.','TDF 300 mg / FTC 200 mg, tab.','TDF 300mg / 3TC 300mg / DTG 50mg, tab.') AND mdd.calculated_end_date > CURRENT_DATE AND mdd.date_stopped IS NULL
	GROUP BY c.patient_id, c.encounter_id_inclusion, c.date_inclusion, c.date_de_sortie, mdd.patient_id
	ORDER BY c.patient_id, c.encounter_id_inclusion, c.date_inclusion, c.date_de_sortie),
-- The last HIV CTE provides the last HIV test result and date per patient, both routine and confirmation test are considered. Only tests with both a date and result are included. If a confirmation test result is present then it is reported, if a confirmation test result is not present then the routine test result is reported. 
dernière_test_vih AS (
	SELECT
		DISTINCT ON (c.patient_id, c.encounter_id_inclusion, c.date_inclusion, c.date_de_sortie) c.encounter_id_inclusion, 
		svil.date_test_vih,
		svil.test_vih 
	FROM cohorte c 
	LEFT OUTER JOIN (SELECT 
			patient_id, 
			CASE WHEN date_de_test_vih_de_confirmation IS NOT NULL AND test_vih_de_confirmation IS NOT NULL THEN date_de_test_vih_de_confirmation WHEN (date_de_test_vih_de_confirmation IS NULL OR test_vih_de_confirmation IS NULL) AND date_de_test_vih_de_routine IS NOT NULL AND test_vih_de_routine IS NOT NULL THEN date_de_test_vih_de_routine ELSE NULL END AS date_test_vih, 
			CASE WHEN date_de_test_vih_de_confirmation IS NOT NULL AND test_vih_de_confirmation IS NOT NULL THEN test_vih_de_confirmation WHEN (date_de_test_vih_de_confirmation IS NULL OR test_vih_de_confirmation IS NULL) AND date_de_test_vih_de_routine IS NOT NULL AND test_vih_de_routine IS NOT NULL THEN test_vih_de_routine ELSE NULL END AS test_vih 
		FROM signes_vitaux_et_informations_laboratoire
		WHERE (date_de_test_vih_de_confirmation IS NOT NULL AND test_vih_de_confirmation IS NOT NULL) OR (date_de_test_vih_de_routine IS NOT NULL AND test_vih_de_routine IS NOT NULL)) svil 
		ON c.patient_id = svil.patient_id AND c.date_inclusion <= svil.date_test_vih::date AND CASE WHEN c.date_de_sortie IS NOT NULL THEN c.date_de_sortie ELSE CURRENT_DATE END >= svil.date_test_vih::date
	GROUP BY c.patient_id, c.encounter_id_inclusion, c.date_inclusion, c.date_de_sortie, svil.date_test_vih, svil.test_vih 
	ORDER BY c.patient_id, c.encounter_id_inclusion, c.date_inclusion, c.date_de_sortie, svil.date_test_vih DESC),	
-- The last CD4 CTE provides the last CD4 result and date per patient. Only tests with both a date and result are included. If the prélèvement date is completed, then this data is reported. If no pélèvement date is completed, then the récéption date is reported. 
dernière_cd4 AS (
	SELECT
		DISTINCT ON (c.patient_id, c.encounter_id_inclusion, c.date_inclusion, c.date_de_sortie) c.encounter_id_inclusion, 
		svil.date_cd4, 
		svil.résultat_brut_cd4,
		svil.résultat_seuil_cd4_cellules_ml
	FROM cohorte c 
	LEFT OUTER JOIN (SELECT 
			patient_id, 
			CASE WHEN date_de_prélèvement_cd4 IS NOT NULL THEN date_de_prélèvement_cd4 WHEN date_de_prélèvement_cd4 IS NULL THEN date_de_récéption_des_résultats_cd4 ELSE NULL END AS date_cd4, 
			_résultat_brut_cd4 AS résultat_brut_cd4,
			résultat_seuil_cd4_cellules_ml
		FROM signes_vitaux_et_informations_laboratoire
		WHERE (date_de_prélèvement_cd4 IS NOT NULL OR date_de_récéption_des_résultats_cd4 IS NOT NULL) AND _résultat_brut_cd4 IS NOT NULL) svil 
		ON c.patient_id = svil.patient_id AND c.date_inclusion <= svil.date_cd4::date AND CASE WHEN c.date_de_sortie IS NOT NULL THEN c.date_de_sortie ELSE CURRENT_DATE END >= svil.date_cd4::date
	GROUP BY c.patient_id, c.encounter_id_inclusion, c.date_inclusion, c.date_de_sortie, date_cd4, résultat_brut_cd4, résultat_seuil_cd4_cellules_ml
	ORDER BY c.patient_id, c.encounter_id_inclusion, c.date_inclusion, c.date_de_sortie, date_cd4 DESC),
-- The last viral load CTE provides the last viral load result and date per patient. Only tests with both a date and result are included. If the prélèvement date is completed, then this data is reported. If no pélèvement date is completed, then the récéption date is reported. 
dernière_charge_virale_vih AS (
	SELECT
		DISTINCT ON (c.patient_id, c.encounter_id_inclusion, c.date_inclusion, c.date_de_sortie) c.encounter_id_inclusion, 
		svil.date_charge_virale_vih, 
		svil.résultat_brut_charge_virale_vih, 
		svil.résultat_seuil_charge_virale_vih
	FROM cohorte c 
	LEFT OUTER JOIN (SELECT 
			patient_id, 
			CASE WHEN date_de_prélèvement_charge_virale_vih IS NOT NULL THEN date_de_prélèvement_charge_virale_vih WHEN date_de_prélèvement_charge_virale_vih IS NULL THEN date_de_réception_des_résultats_charge_virale_vih ELSE NULL END AS date_charge_virale_vih, 
			résultat_brut_charge_virale_vih,
			résultat_seuil_charge_virale_vih
		FROM signes_vitaux_et_informations_laboratoire
		WHERE (date_de_prélèvement_charge_virale_vih IS NOT NULL OR date_de_réception_des_résultats_charge_virale_vih IS NOT NULL) AND résultat_brut_charge_virale_vih IS NOT NULL) svil 
		ON c.patient_id = svil.patient_id AND c.date_inclusion <= svil.date_charge_virale_vih::date AND CASE WHEN c.date_de_sortie IS NOT NULL THEN c.date_de_sortie ELSE CURRENT_DATE END >= svil.date_charge_virale_vih::date
	GROUP BY c.patient_id, c.encounter_id_inclusion, c.date_inclusion, c.date_de_sortie, date_charge_virale_vih, résultat_brut_charge_virale_vih, résultat_seuil_charge_virale_vih	
	ORDER BY c.patient_id, c.encounter_id_inclusion, c.date_inclusion, c.date_de_sortie, date_charge_virale_vih DESC),
-- The last blood pressure CTE extracts the last complete blood pressure measurements reported per cohort enrollment. Only blood pressures with a date, systolic, and diastolic information are reported.
dernière_pression_artérielle AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.encounter_id_inclusion, c.date_inclusion, c.date_de_sortie) c.encounter_id_inclusion, 
		svil.date AS date_dernière_pression_artérielle,
		svil.tension_arterielle_systolique AS dernière_pression_artérielle_systolique,
		svil.tension_arterielle_diastolique AS dernière_pression_artérielle_diastolique
	FROM cohorte c
	LEFT OUTER JOIN signes_vitaux_et_informations_laboratoire svil
		ON c.patient_id = svil.patient_id AND c.date_inclusion <= svil.date::date AND CASE WHEN c.date_de_sortie IS NOT NULL THEN c.date_de_sortie ELSE current_date END >= svil.date::date
	WHERE svil.date IS NOT NULL AND svil.tension_arterielle_systolique IS NOT NULL AND svil.tension_arterielle_diastolique IS NOT NULL
	ORDER BY c.patient_id, c.encounter_id_inclusion, c.date_inclusion, c.date_de_sortie, svil.date DESC),
-- The last BMI CTE extracts the last BMI measurement reported per cohort enrollment. Only BMI records with a date, weight, and height are reported.
dernère_imc AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.encounter_id_inclusion, c.date_inclusion, c.date_de_sortie) c.encounter_id_inclusion, 
		svil.date AS date_dernière_imc,
		svil.indice_de_masse_corporelle AS dernière_imc
	FROM cohorte c
	LEFT OUTER JOIN signes_vitaux_et_informations_laboratoire svil
		ON c.patient_id = svil.patient_id AND c.date_inclusion <= svil.date::date AND CASE WHEN c.date_de_sortie IS NOT NULL THEN c.date_de_sortie ELSE current_date END >= svil.date::date
	WHERE svil.date IS NOT NULL AND svil.indice_de_masse_corporelle IS NOT NULL
	ORDER BY c.patient_id, c.encounter_id_inclusion, c.date_inclusion, c.date_de_sortie, svil.date DESC)
-- Main query --
SELECT
	pi."Patient_Identifier",
	c.patient_id,
	c.encounter_id_inclusion,
	pa."Identifiant_cohorte",
	pdd.age AS age_actuel,
	CASE 
		WHEN pdd.age::int <= 4 THEN '0-4'
		WHEN pdd.age::int >= 5 AND pdd.age::int <= 14 THEN '05-14'
		WHEN pdd.age::int >= 15 AND pdd.age::int <= 24 THEN '15-24'
		WHEN pdd.age::int >= 25 AND pdd.age::int <= 34 THEN '25-34'
		WHEN pdd.age::int >= 35 AND pdd.age::int <= 44 THEN '35-44'
		WHEN pdd.age::int >= 45 AND pdd.age::int <= 54 THEN '45-54'
		WHEN pdd.age::int >= 55 AND pdd.age::int <= 64 THEN '55-64'
		WHEN pdd.age::int >= 65 THEN '65+'
		ELSE NULL
	END AS groupe_age_actuel,
	EXTRACT(YEAR FROM (SELECT age(c.date_inclusion, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))) AS age_inclusion,
	CASE 
		WHEN EXTRACT(YEAR FROM (SELECT age(c.date_inclusion, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))))::int <= 4 THEN '0-4'
		WHEN EXTRACT(YEAR FROM (SELECT age(c.date_inclusion, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))))::int >= 5 AND EXTRACT(YEAR FROM (SELECT age(c.date_inclusion, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))) <= 14 THEN '05-14'
		WHEN EXTRACT(YEAR FROM (SELECT age(c.date_inclusion, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))))::int >= 15 AND EXTRACT(YEAR FROM (SELECT age(c.date_inclusion, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))) <= 24 THEN '15-24'
		WHEN EXTRACT(YEAR FROM (SELECT age(c.date_inclusion, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))))::int >= 25 AND EXTRACT(YEAR FROM (SELECT age(c.date_inclusion, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))) <= 34 THEN '25-34'
		WHEN EXTRACT(YEAR FROM (SELECT age(c.date_inclusion, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))))::int >= 35 AND EXTRACT(YEAR FROM (SELECT age(c.date_inclusion, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))) <= 44 THEN '35-44'
		WHEN EXTRACT(YEAR FROM (SELECT age(c.date_inclusion, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))))::int >= 45 AND EXTRACT(YEAR FROM (SELECT age(c.date_inclusion, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))) <= 54 THEN '45-54'
		WHEN EXTRACT(YEAR FROM (SELECT age(c.date_inclusion, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))))::int >= 55 AND EXTRACT(YEAR FROM (SELECT age(c.date_inclusion, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))) <= 64 THEN '55-64'
		WHEN EXTRACT(YEAR FROM (SELECT age(c.date_inclusion, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))))::int >= 65 THEN '65+'
		ELSE NULL
	END AS groupe_age_inclusion,
	pdd.gender AS sexe,
	CASE 
		WHEN pa."Civil_status" = 'Never married' THEN 'Célibataire' 
		WHEN pa."Civil_status" = 'Living together' THEN 'Concubinage' 
		WHEN pa."Civil_status" = 'Married' THEN 'Marié' 
		WHEN pa."Civil_status" = 'Widowed' THEN 'Veuf(ve)' 
		WHEN pa."Civil_status" = 'Separated' THEN 'Séparé' 
		WHEN pa."Civil_status" = 'Other' THEN 'Autre' 
	ELSE NULL END AS statut_civil,
	CASE 
		WHEN pa."Education_level" = 'No formal education' THEN 'Pas éducation formelle'
		WHEN pa."Education_level" = 'Intermittent schooling' THEN 'Scolarisation intermittente'  
		WHEN pa."Education_level" = 'Primary school education' THEN 'École primaire'  
		WHEN pa."Education_level" = 'High school' THEN 'École secondaire'  
		WHEN pa."Education_level" = 'College/University' THEN 'Collège/Université' 
	ELSE NULL END AS niveau_education,
	CASE 
		WHEN pa."Occupation" = 'Employed' THEN 'oui - rémunéré'
		WHEN pa."Occupation" = 'Retired' THEN 'oui - non rémunéré'
		WHEN pa."Occupation" = 'No' THEN 'non, Autre'
	ELSE NULL END AS activite,
	CASE 
		WHEN pa."Living_conditions" = 'Unstable accommodation' THEN 'Logement instable'
		WHEN pa."Living_conditions" = 'Stable accommodation' THEN 'Logement stable'
		WHEN pa."Living_conditions" LIKE 'Lives at relatives/friends' THEN 'Vit chez des parents/amis'
		WHEN pa."Living_conditions" = 'In transit' THEN 'En transit/déménagement'
		WHEN pa."Living_conditions" = 'Homeless' THEN 'Sans domicile fixe'
		WHEN pa."Living_conditions" = 'Other' THEN 'Autre'
	ELSE NULL END AS condition_habitation,
	c.date_inclusion AS date_inclusion,
	CASE WHEN c.date_de_sortie IS NULL THEN 'Oui' END AS en_cohorte,
	CASE WHEN ((DATE_PART('year', CURRENT_DATE) - DATE_PART('year', c.date_inclusion)) * 12 + (DATE_PART('month', CURRENT_DATE) - DATE_PART('month', c.date_inclusion))) >= 6 AND c.date_de_sortie IS NULL THEN 'Oui' END AS en_cohorte_6m,
	CASE WHEN ((DATE_PART('year', CURRENT_DATE) - DATE_PART('year', c.date_inclusion)) * 12 + (DATE_PART('month', CURRENT_DATE) - DATE_PART('month', c.date_inclusion))) >= 12 AND c.date_de_sortie IS NULL THEN 'Oui' END AS en_cohorte_12m,
	c.readmission,
	c.lieu_de_visite_inclusion,
	CASE WHEN dp.date_derniere_ptpe IS NOT NULL THEN 'Oui' ELSE NULL END AS ptpe,
	lfl.dernière_fiche_location,
	lf.date_derniere_visite,
	lf.dernière_fiche_type,
	CASE WHEN lf.date_derniere_visite < (CURRENT_DATE - INTERVAL '90 DAYS') THEN 'Oui' ELSE NULL END AS sans_visite_90j,
	c.date_de_sortie,
	c.statut_de_sortie,
	CASE 
		WHEN lndx.mnt IS NOT NULL AND lndx.tb IS NULL AND lndx.vih IS NULL AND lndx.troubles_de_santé_mentale IS NULL THEN 'MNT' 
		WHEN lndx.mnt IS NULL AND lndx.tb IS NOT NULL AND lndx.vih IS NULL AND lndx.troubles_de_santé_mentale IS NULL THEN 'TB' 
		WHEN lndx.mnt IS NULL AND lndx.tb IS NULL AND lndx.vih IS NOT NULL AND lndx.troubles_de_santé_mentale IS NULL THEN 'VIH' 
		WHEN lndx.mnt IS NULL AND lndx.tb IS NULL AND lndx.vih IS NULL AND lndx.troubles_de_santé_mentale IS NOT NULL THEN 'Santé mentale' 
		WHEN lndx.mnt IS NOT NULL AND lndx.tb IS NULL AND lndx.vih IS NOT NULL AND lndx.troubles_de_santé_mentale IS NULL THEN 'MNT + VIH' 
		WHEN lndx.mnt IS NOT NULL AND lndx.tb IS NOT NULL AND lndx.vih IS NOT NULL AND lndx.troubles_de_santé_mentale IS NULL THEN 'VIH + TB' 
	ELSE NULL END AS cohorte,
	lndx.asthme::date,
	lndx.drépanocytose,
	lndx.insuffisance_renal_chronique,
	lndx.maladie_cardiovasculaire,
	lndx.bronchopneumopathie_chronique_obstructive,
	lndx.diabète_type1,
	lndx.diabète_type2,
	lndx.hypertension,
	lndx.hypothyroïdie,
	lndx.hyperthyroïdie,
	lndx.épilepsie_focale,
	lndx.épilepsie_généralisée,
	lndx.épilepsie_non_classifiée,
	lndx.tb_pulmonaire,
	lndx.tb_extrapulmonaire,
	lndx.vih,
	lndx.troubles_de_santé_mentale,
	lndx.autre_diagnostic,
	lndl.liste_diagnostic,
	frp.médecine_traditionnelle,
	frp.tabagisme_passif,
	frp.fumeur,
	frp.consommation_alcool,
	frp.autre_facteurs_risque,
	iarv.date_instauration_arv,
	marv.traitement_arv_actuellement,
	dtv.date_test_vih,
	dtv.test_vih,
	dc.date_cd4, 
	dc.résultat_brut_cd4,
	dc.résultat_seuil_cd4_cellules_ml,
	dcv.date_charge_virale_vih, 
	dcv.résultat_brut_charge_virale_vih, 
	dcv.résultat_seuil_charge_virale_vih, 
	dpa.date_dernière_pression_artérielle,
	dpa.dernière_pression_artérielle_systolique,
	dpa.dernière_pression_artérielle_diastolique,
	CASE WHEN dpa.dernière_pression_artérielle_systolique IS NOT NULL AND dpa.dernière_pression_artérielle_diastolique IS NOT NULL THEN CONCAT(dpa.dernière_pression_artérielle_systolique,'/',dpa.dernière_pression_artérielle_diastolique) END AS dernière_pression_artérielle,
	CASE WHEN dpa.dernière_pression_artérielle_systolique <= 140 AND dpa.dernière_pression_artérielle_diastolique <= 90 THEN 'Oui' WHEN dpa.dernière_pression_artérielle_systolique > 140 OR dpa.dernière_pression_artérielle_diastolique > 90 THEN 'Non' END AS dernière_tension_artérielle_controlée,
	dimc.date_dernière_imc,
	dimc.dernière_imc
FROM cohorte c
LEFT OUTER JOIN patient_identifier pi
	ON c.patient_id = pi.patient_id
LEFT OUTER JOIN person_attributes pa
	ON c.patient_id = pa.person_id
LEFT OUTER JOIN person_details_default pdd 
	ON c.patient_id = pdd.person_id
LEFT OUTER JOIN dernière_ptpe dp
	ON c.encounter_id_inclusion = dp.encounter_id_inclusion
LEFT OUTER JOIN dernière_fiche lf
	ON c.encounter_id_inclusion = lf.encounter_id_inclusion
LEFT OUTER JOIN dernière_fiche_location lfl
	ON c.encounter_id_inclusion = lfl.encounter_id_inclusion
LEFT OUTER JOIN diagnostic_cohorte_pivot lndx
	ON c.encounter_id_inclusion = lndx.encounter_id_inclusion
LEFT OUTER JOIN diagnostic_cohorte_liste lndl
	ON c.encounter_id_inclusion = lndl.encounter_id_inclusion
LEFT OUTER JOIN facteurs_risque_pivot frp 
	ON c.encounter_id_inclusion = frp.encounter_id_inclusion
LEFT OUTER JOIN instauration_arv iarv 
	ON c.encounter_id_inclusion = iarv.encounter_id_inclusion
LEFT OUTER JOIN médicament_arv marv 
	ON c.encounter_id_inclusion = marv.encounter_id_inclusion
LEFT OUTER JOIN dernière_test_vih dtv
	ON c.encounter_id_inclusion = dtv.encounter_id_inclusion
LEFT OUTER JOIN dernière_cd4 dc
	ON c.encounter_id_inclusion = dc.encounter_id_inclusion
LEFT OUTER JOIN dernière_charge_virale_vih dcv
	ON c.encounter_id_inclusion = dcv.encounter_id_inclusion
LEFT OUTER JOIN dernière_pression_artérielle dpa
	ON c.encounter_id_inclusion = dpa.encounter_id_inclusion
LEFT OUTER JOIN dernère_imc dimc
	ON c.encounter_id_inclusion = dimc.encounter_id_inclusion;