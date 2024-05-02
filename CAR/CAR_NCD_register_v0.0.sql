-- The first CTE build the frame for patients entering and exiting the cohort. This frame is based on MNT VIH TB form with visit types of 'Inclusion' and 'Sortie'. The query takes all initial visit dates and matches discharge visit dates if the discharge visit date falls between the initial visit date and the next initial visit date (if present).
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
-- The last completed form CTE dlooks at the last type of visit for each patient based on the clinical forms.  
dernière_fiche AS (
	SELECT 
		DISTINCT ON (c.patient_id, c.encounter_id_inclusion, c.date_inclusion, c.date_de_sortie) c.encounter_id_inclusion,
		nvsl.date AS date_derniere_visite,
		nvsl.dernière_fiche_type,
		CASE WHEN nvsl.form_field_path = 'NCD2' THEN 'MNT/VIH/TB' WHEN nvsl.form_field_path = 'Vitals and laboratory information' THEN 'signes vitaux et informations laboratoire' WHEN nvsl.form_field_path = 'PMTCT' THEN 'PTPE' ELSE NULL END AS type_derniere_fiche
	FROM cohorte c
	LEFT OUTER JOIN (SELECT patient_id, CASE WHEN type_de_visite = 'Sortie' AND date_de_sortie IS NOT NULL THEN date_de_sortie 
	ELSE date END AS date, type_de_visite AS dernière_fiche_type, form_field_path FROM mnt_vih_tb UNION SELECT patient_id, date, type_de_visite AS dernière_fiche_type, form_field_path FROM ptpe UNION SELECT patient_id, GREATEST(date, date_de_prélèvement, tb_date_de_prélèvement, date_d_examen_radiologique, date_de_test_vih_de_routine, date_de_test_vih_de_confirmation, date_de_prélèvement_charge_virale_vih, date_de_réception_des_résultats_charge_virale_vih, date_de_prélèvement_cd4, date_de_récéption_des_résultats_cd4, date_de_prélèvement_de_test_de_résistance_génotypique_gène) AS date, form_field_path AS dernière_fiche_type, form_field_path FROM signes_vitaux_et_informations_laboratoire) nvsl
		ON c.patient_id = nvsl.patient_id AND c.date_inclusion <= nvsl.date::date AND CASE WHEN c.date_de_sortie IS NOT NULL THEN c.date_de_sortie ELSE current_date END >= nvsl.date::date
	GROUP BY c.patient_id, c.encounter_id_inclusion, c.date_inclusion, c.date_de_sortie, nvsl.date, nvsl.dernière_fiche_type, nvsl.form_field_path
	ORDER BY c.patient_id, c.encounter_id_inclusion, c.date_inclusion, c.date_de_sortie, nvsl.date DESC),
-- The diagnosis CTEs select the last reported NCD diagnosis per cohort enrollment and pivots the data horizontally.
diagnostic_cohorte AS (
	SELECT
		d.patient_id, c.encounter_id_inclusion, n.date, d.diagnostic
	FROM diagnostic d 
	LEFT JOIN mnt_vih_tb n USING(encounter_id)
	LEFT JOIN cohorte c ON d.patient_id = c.patient_id AND c.date_inclusion <= n.date AND CASE WHEN c.date_de_sortie IS NOT NULL THEN c.date_de_sortie ELSE current_date END >= n.date),
dernière_diagnostic_cohorte AS (
	SELECT cd.patient_id, cd.encounter_id_inclusion, cd.date, cd.diagnostic
	FROM diagnostic_cohorte cd
	INNER JOIN (SELECT encounter_id_inclusion, MAX(date) AS max_date FROM diagnostic_cohorte GROUP BY encounter_id_inclusion) cd2 ON cd.encounter_id_inclusion = cd2.encounter_id_inclusion AND cd.date = cd2.max_date),
dernière_diagnostic_cohorte_pivot AS (
	SELECT 
		DISTINCT ON (encounter_id_inclusion, patient_id, date) encounter_id_inclusion, 
		patient_id, 
		date,
		MAX (CASE WHEN diagnostic = 'Asthme' THEN 1 ELSE NULL END) AS asthme,
		MAX (CASE WHEN diagnostic = 'Drépanocytose' THEN 1 ELSE NULL END) AS drépanocytose,
		MAX (CASE WHEN diagnostic = 'Insuffisance renale chronique' THEN 1 ELSE NULL END) AS insuffisance_renal_chronique,
		MAX (CASE WHEN diagnostic = 'Maladie cardiovasculaire' THEN 1 ELSE NULL END) AS maladie_cardiovasculaire,
		MAX (CASE WHEN diagnostic = 'Bronchopneumopathie chronique obstructive' THEN 1 ELSE NULL END) AS bronchopneumopathie_chronique_obstructive,
		MAX (CASE WHEN diagnostic = 'Diabète sucré de type 1' THEN 1 ELSE NULL END) AS diabète_type1,
		MAX (CASE WHEN diagnostic = 'Diabète sucré de type 2' THEN 1 ELSE NULL END) AS diabète_type2,
		MAX (CASE WHEN diagnostic = 'Hypertension' THEN 1 ELSE NULL END) AS hypertension,
		MAX (CASE WHEN diagnostic = 'Hypothyroïdie' THEN 1 ELSE NULL END) AS hypothyroïdie,
		MAX (CASE WHEN diagnostic = 'Hyperthyroïdie' THEN 1 ELSE NULL END) AS hyperthyroïdie,
		MAX (CASE WHEN diagnostic = 'Épilepsie focale' THEN 1 ELSE NULL END) AS épilepsie_focale,
		MAX (CASE WHEN diagnostic = 'Épilepsie généralisée' THEN 1 ELSE NULL END) AS épilepsie_généralisée,
		MAX (CASE WHEN diagnostic = 'Épilepsie non classifiée' THEN 1 ELSE NULL END) AS épilepsie_non_classifiée,
		MAX (CASE WHEN diagnostic = 'Tuberculose pulmonaire' THEN 1 ELSE NULL END) AS tb_pulmonaire,
		MAX (CASE WHEN diagnostic = 'Tuberculose extrapulmonaire' THEN 1 ELSE NULL END) AS tb_extrapulmonaire,
		MAX (CASE WHEN diagnostic = 'Infection par le VIH' THEN 1 ELSE NULL END) AS vih,
		MAX (CASE WHEN diagnostic = 'Troubles de santé mentale' THEN 1 ELSE NULL END) AS troubles_de_santé_mentale,
		MAX (CASE WHEN diagnostic = 'Autre' THEN 1 ELSE NULL END) AS autre_diagnostic	
	FROM dernière_diagnostic_cohorte
	GROUP BY encounter_id_inclusion, patient_id, date),
dernière_diagnostic_cohorte_liste AS (
	SELECT encounter_id_inclusion, STRING_AGG(diagnostic, ', ') AS liste_diagnostic
	FROM dernière_diagnostic_cohorte
	GROUP BY encounter_id_inclusion),
-- The last visit location CTE finds the last visit location reported in clinical forms.
dernière_fiche_location AS (	
	SELECT 
		DISTINCT ON (c.patient_id, c.encounter_id_inclusion, c.date_inclusion, c.date_de_sortie) c.encounter_id_inclusion,
		nvsl.lieu_de_visite AS dernière_fiche_location
	FROM cohorte c
	LEFT OUTER JOIN (SELECT patient_id, CASE WHEN type_de_visite = 'Sortie' AND date_de_sortie IS NOT NULL THEN date_de_sortie ELSE date END AS date, lieu_de_visite FROM mnt_vih_tb UNION 
	SELECT patient_id, date, lieu_de_visite FROM ptpe) nvsl
		ON c.patient_id = nvsl.patient_id AND c.date_inclusion <= nvsl.date::date AND CASE WHEN c.date_de_sortie IS NOT NULL THEN c.date_de_sortie ELSE current_date END >= nvsl.date::date
	WHERE nvsl.lieu_de_visite IS NOT NULL
	GROUP BY c.patient_id, c.encounter_id_inclusion, c.date_inclusion, c.date_de_sortie, nvsl.date, nvsl.lieu_de_visite
	ORDER BY c.patient_id, c.encounter_id_inclusion, c.date_inclusion, c.date_de_sortie, nvsl.date DESC)
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
	EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))) AS age_inclusion,
	CASE 
		WHEN EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))))::int <= 4 THEN '0-4'
		WHEN EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))))::int >= 5 AND EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))) <= 14 THEN '05-14'
		WHEN EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))))::int >= 15 AND EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))) <= 24 THEN '15-24'
		WHEN EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))))::int >= 25 AND EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))) <= 34 THEN '25-34'
		WHEN EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))))::int >= 35 AND EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))) <= 44 THEN '35-44'
		WHEN EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))))::int >= 45 AND EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))) <= 54 THEN '45-54'
		WHEN EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))))::int >= 55 AND EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy')))) <= 64 THEN '55-64'
		WHEN EXTRACT(YEAR FROM (SELECT age(ped.encounter_datetime, TO_DATE(CONCAT('01-01-', pdd.birthyear), 'dd-MM-yyyy'))))::int >= 65 THEN '65+'
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
		WHEN pa."Occupation" = '' THEN 'oui – rémunéré'
		WHEN pa."Occupation" = '' THEN 'oui – non rémunéré'
		WHEN pa."Occupation" = '' THEN 'non, Autre'
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
	lfl.dernière_fiche_location,
	lf.date_derniere_visite,
	lf.dernière_fiche_type,	
	c.date_de_sortie,
	c.statut_de_sortie,
	lndx.asthme,
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
	lndl.liste_diagnostic
FROM cohorte c
LEFT OUTER JOIN patient_identifier pi
	ON c.patient_id = pi.patient_id
LEFT OUTER JOIN person_attributes pa
	ON c.patient_id = pa.person_id
LEFT OUTER JOIN person_details_default pdd 
	ON c.patient_id = pdd.person_id
LEFT OUTER JOIN patient_encounter_details_default ped 
	ON c.encounter_id_inclusion = ped.encounter_id
LEFT OUTER JOIN dernière_fiche lf
	ON c.encounter_id_inclusion = lf.encounter_id_inclusion
LEFT OUTER JOIN dernière_diagnostic_cohorte_pivot lndx
	ON c.encounter_id_inclusion = lndx.encounter_id_inclusion
LEFT OUTER JOIN dernière_diagnostic_cohorte_liste lndl
	ON c.encounter_id_inclusion = lndl.encounter_id_inclusion
LEFT OUTER JOIN dernière_fiche_location lfl
	ON c.encounter_id_inclusion = lfl.encounter_id_inclusion;