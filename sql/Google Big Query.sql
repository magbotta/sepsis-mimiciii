WITH 
icustay AS (SELECT HADM_ID, SUM(LOS) AS LOS_ICU FROM `physionet-data.mimiciii_clinical.icustays` GROUP BY HADM_ID), 
callout AS (SELECT HADM_ID, COUNT(HADM_ID) AS CALLOUT_COUNT FROM  `physionet-data.mimiciii_clinical.callout` GROUP BY HADM_ID),
diags AS (SELECT HADM_ID, COUNT(HADM_ID) AS DIAG_COUNT FROM `physionet-data.mimiciii_clinical.diagnoses_icd` GROUP BY HADM_ID),
preps AS (SELECT HADM_ID, COUNT(HADM_ID) AS PRES_COUNT FROM `physionet-data.mimiciii_clinical.prescriptions` GROUP BY HADM_ID),
procs AS (SELECT HADM_ID, COUNT(HADM_ID) AS PROC_COUNT FROM `physionet-data.mimiciii_clinical.procedures_icd` GROUP BY HADM_ID),
cpts AS (SELECT HADM_ID, COUNT(HADM_ID) AS CPT_COUNT FROM `physionet-data.mimiciii_clinical.cptevents` GROUP BY HADM_ID),
labs AS (SELECT HADM_ID, COUNT(HADM_ID) AS LAB_COUNT FROM `physionet-data.mimiciii_clinical.labevents` GROUP BY HADM_ID),
inputs_cv AS (SELECT HADM_ID, COUNT(HADM_ID) AS INPUTS_CV_COUNT FROM `physionet-data.mimiciii_clinical.inputevents_cv` GROUP BY HADM_ID),
inputs_mv AS (SELECT HADM_ID, COUNT(HADM_ID) AS INPUTS_MV_COUNT FROM `physionet-data.mimiciii_clinical.inputevents_mv` GROUP BY HADM_ID),
outputs AS (SELECT HADM_ID, COUNT(HADM_ID) AS OUTPUT_COUNT FROM `physionet-data.mimiciii_clinical.outputevents` GROUP BY HADM_ID),
transfers AS (SELECT HADM_ID, COUNT(HADM_ID) AS TRANSFER_COUNT FROM `physionet-data.mimiciii_clinical.transfers` GROUP BY HADM_ID),
micros AS (SELECT HADM_ID, COUNT(HADM_ID) AS MICRO_COUNT FROM `physionet-data.mimiciii_clinical.microbiologyevents` GROUP BY HADM_ID),
diafeature AS (WITH co_dx AS
(
	SELECT subject_id, hadm_id
  , MAX(
    	CASE
        -- septicemia
    		WHEN substring(icd9_code,1,3) = '038' THEN 1
        -- septicemic, bacteremia, disseminated fungal infection, disseminated candida infection
				-- NOTE: the paper specifies 020.0 ... but this is bubonic plague
				-- presumably, they meant 020.2, which is septicemic plague
        WHEN substring(icd9_code,1,4) in ('0202','7907','1179','1125') THEN 1
        -- disseminated fungal endocarditis
        WHEN substring(icd9_code,1,5) = '11281' THEN 1
      ELSE 0 END
    ) AS sepsis
    , MAX(
      CASE
        WHEN substring(icd9_code,1,4) in ('7991') THEN 1
        WHEN substring(icd9_code,1,5) in ('51881','51882','51885','78609') THEN 1
      ELSE 0 END
    ) AS respiratory
    , MAX(
      CASE
        WHEN substring(icd9_code,1,4) in ('4580','7855','4580','4588','4589','7963') THEN 1
        WHEN substring(icd9_code,1,5) in ('785.51','785.59') THEN 1
      ELSE 0 END
    ) AS cardiovascular
    , MAX(
      CASE
        WHEN substring(icd9_code,1,3) in ('584','580','585') THEN 1
      ELSE 0 END
    ) AS renal
    , MAX(
      CASE
        WHEN substring(icd9_code,1,3) in ('570') THEN 1
        WHEN substring(icd9_code,1,4) in ('5722','5733') THEN 1
      ELSE 0 END
    ) AS hepatic
    , MAX(
      CASE
        WHEN substring(icd9_code,1,4) in ('2862','2866','2869','2873','2874','2875') THEN 1
      ELSE 0 END
    ) AS hematologic
    , MAX(
      CASE
        WHEN substring(icd9_code,1,4) in ('2762') THEN 1
      ELSE 0 END
    ) AS metabolic
    , MAX(
      CASE
        WHEN substring(icd9_code,1,3) in ('293') THEN 1
        WHEN substring(icd9_code,1,4) in ('3481','3483') THEN 1
        WHEN substring(icd9_code,1,5) in ('78001','78009') THEN 1
      ELSE 0 END
    ) AS neurologic
  FROM `physionet-data.mimiciii_clinical.diagnoses_icd`
  GROUP BY subject_id, hadm_id
)
-- procedure codes:
-- "96.7 - Ventilator management"
-- translated:
--    9670	Continuous invasive mechanical ventilation of unspecified duration
--    9671	Continuous invasive mechanical ventilation for less than 96 consecutive hours
--    9672	Continuous invasive mechanical ventilation for 96 consecutive hours or more
-- "39.95 - Hemodialysis"
--    3995	Hemodialysis
-- "89.14 - Electroencephalography"
--    8914	Electroencephalogram
, co_proc as
(
  SELECT subject_id, hadm_id
  , MAX(CASE WHEN substring(icd9_code,1,3) = '967' then 1 ELSE 0 END) as respiratory
  , MAX(CASE WHEN substring(icd9_code,1,4) = '3995' then 1 ELSE 0 END) as renal
  , MAX(CASE WHEN substring(icd9_code,1,4) = '8914' then 1 ELSE 0 END) as neurologic
  FROM  `physionet-data.mimiciii_clinical.procedures_icd`
  GROUP BY subject_id, hadm_id
)
select adm.subject_id, adm.hadm_id
, co_dx.sepsis
, CASE
    WHEN co_dx.respiratory = 1 OR co_proc.respiratory = 1
      OR co_dx.cardiovascular = 1
      OR co_dx.renal = 1 OR co_proc.renal = 1
      OR co_dx.hepatic = 1
      OR co_dx.hematologic = 1
      OR co_dx.metabolic = 1
      OR co_dx.neurologic = 1 OR co_proc.neurologic = 1
    THEN 1
  ELSE 0 END as organ_failure
, case when co_dx.respiratory = 1 or co_proc.respiratory = 1 then 1 else 0 end as respiratory
, co_dx.cardiovascular
, case when co_dx.renal = 1 or co_proc.renal = 1 then 1 else 0 end as renal
, co_dx.hepatic
, co_dx.hematologic
, co_dx.metabolic
, case when co_dx.neurologic = 1 or co_proc.neurologic = 1 then 1 else 0 end as neurologic
FROM `physionet-data.mimiciii_clinical.admissions` adm
left join co_dx
  on adm.hadm_id = co_dx.hadm_id
left join co_proc
  on adm.hadm_id = co_proc.hadm_id),
vitals as (
SELECT pvt.subject_id, pvt.hadm_id, pvt.icustay_id
-- Easier names
, min(case when VitalID = 1 then valuenum else null end) as HeartRate_Min
, max(case when VitalID = 1 then valuenum else null end) as HeartRate_Max
, avg(case when VitalID = 1 then valuenum else null end) as HeartRate_Mean
, min(case when VitalID = 2 then valuenum else null end) as SysBP_Min
, max(case when VitalID = 2 then valuenum else null end) as SysBP_Max
, avg(case when VitalID = 2 then valuenum else null end) as SysBP_Mean
, min(case when VitalID = 3 then valuenum else null end) as DiasBP_Min
, max(case when VitalID = 3 then valuenum else null end) as DiasBP_Max
, avg(case when VitalID = 3 then valuenum else null end) as DiasBP_Mean
, min(case when VitalID = 4 then valuenum else null end) as MeanBP_Min
, max(case when VitalID = 4 then valuenum else null end) as MeanBP_Max
, avg(case when VitalID = 4 then valuenum else null end) as MeanBP_Mean
, min(case when VitalID = 5 then valuenum else null end) as RespRate_Min
, max(case when VitalID = 5 then valuenum else null end) as RespRate_Max
, avg(case when VitalID = 5 then valuenum else null end) as RespRate_Mean
, min(case when VitalID = 6 then valuenum else null end) as TempC_Min
, max(case when VitalID = 6 then valuenum else null end) as TempC_Max
, avg(case when VitalID = 6 then valuenum else null end) as TempC_Mean
, min(case when VitalID = 7 then valuenum else null end) as SpO2_Min
, max(case when VitalID = 7 then valuenum else null end) as SpO2_Max
, avg(case when VitalID = 7 then valuenum else null end) as SpO2_Mean
, min(case when VitalID = 8 then valuenum else null end) as Glucose_Min
, max(case when VitalID = 8 then valuenum else null end) as Glucose_Max
, avg(case when VitalID = 8 then valuenum else null end) as Glucose_Mean

FROM  (
  select ie.subject_id, ie.hadm_id, ie.icustay_id
  , case
    when itemid in (211,220045) and valuenum > 0 and valuenum < 300 then 1 -- HeartRate
    when itemid in (51,442,455,6701,220179,220050) and valuenum > 0 and valuenum < 400 then 2 -- SysBP
    when itemid in (8368,8440,8441,8555,220180,220051) and valuenum > 0 and valuenum < 300 then 3 -- DiasBP
    when itemid in (456,52,6702,443,220052,220181,225312) and valuenum > 0 and valuenum < 300 then 4 -- MeanBP
    when itemid in (615,618,220210,224690) and valuenum > 0 and valuenum < 70 then 5 -- RespRate
    when itemid in (223761,678) and valuenum > 70 and valuenum < 120  then 6 -- TempF, converted to degC in valuenum call
    when itemid in (223762,676) and valuenum > 10 and valuenum < 50  then 6 -- TempC
    when itemid in (646,220277) and valuenum > 0 and valuenum <= 100 then 7 -- SpO2
    when itemid in (807,811,1529,3745,3744,225664,220621,226537) and valuenum > 0 then 8 -- Glucose

    else null end as VitalID
      -- convert F to C
  , case when itemid in (223761,678) then (valuenum-32)/1.8 else valuenum end as valuenum

  from `physionet-data.mimiciii_clinical.icustays`  ie
  left join  `physionet-data.mimiciii_clinical.chartevents` ce
  on ie.subject_id = ce.subject_id and ie.hadm_id = ce.hadm_id and ie.icustay_id = ce.icustay_id
  and ce.charttime between ie.intime and DATE_ADD(ie.intime , INTERVAL 1 day)
  -- exclude rows marked as error
  and ce.error IS DISTINCT FROM 1
  where ce.itemid in
  (
  -- HEART RATE
  211, --"Heart Rate"
  220045, --"Heart Rate"

  -- Systolic/diastolic

  51, --	Arterial BP [Systolic]
  442, --	Manual BP [Systolic]
  455, --	NBP [Systolic]
  6701, --	Arterial BP #2 [Systolic]
  220179, --	Non Invasive Blood Pressure systolic
  220050, --	Arterial Blood Pressure systolic

  8368, --	Arterial BP [Diastolic]
  8440, --	Manual BP [Diastolic]
  8441, --	NBP [Diastolic]
  8555, --	Arterial BP #2 [Diastolic]
  220180, --	Non Invasive Blood Pressure diastolic
  220051, --	Arterial Blood Pressure diastolic


  -- MEAN ARTERIAL PRESSURE
  456, --"NBP Mean"
  52, --"Arterial BP Mean"
  6702, --	Arterial BP Mean #2
  443, --	Manual BP Mean(calc)
  220052, --"Arterial Blood Pressure mean"
  220181, --"Non Invasive Blood Pressure mean"
  225312, --"ART BP mean"

  -- RESPIRATORY RATE
  618,--	Respiratory Rate
  615,--	Resp Rate (Total)
  220210,--	Respiratory Rate
  224690, --	Respiratory Rate (Total)


  -- SPO2, peripheral
  646, 220277,

  -- GLUCOSE, both lab and fingerstick
  807,--	Fingerstick Glucose
  811,--	Glucose (70-105)
  1529,--	Glucose
  3745,--	BloodGlucose
  3744,--	Blood Glucose
  225664,--	Glucose finger stick
  220621,--	Glucose (serum)
  226537,--	Glucose (whole blood)

  -- TEMPERATURE
  223762, -- "Temperature Celsius"
  676,	-- "Temperature C"
  223761, -- "Temperature Fahrenheit"
  678 --	"Temperature F"

  )
) pvt
group by pvt.subject_id, pvt.hadm_id, pvt.icustay_id
order by pvt.subject_id, pvt.hadm_id, pvt.icustay_id)  


SELECT adm.SUBJECT_ID, adm.HADM_ID, adm.ADMISSION_TYPE, 
adm.ADMITTIME, adm.DISCHTIME, adm.DEATHTIME, adm.HOSPITAL_EXPIRE_FLAG, 
adm.DIAGNOSIS, adm.MARITAL_STATUS, adm.INSURANCE, adm.LANGUAGE, adm.RELIGION, adm.ETHNICITY,
pts.GENDER, pts.DOB, 
icustay.LOS_ICU, callout.CALLOUT_COUNT, diags.DIAG_COUNT, preps.PRES_COUNT, procs.PROC_COUNT, cpts.CPT_COUNT, labs.LAB_COUNT, inputs_cv.INPUTS_CV_COUNT, inputs_mv.INPUTS_MV_COUNT, outputs.OUTPUT_COUNT, transfers.TRANSFER_COUNT, micros.MICRO_COUNT, diafeature.SEPSIS , diafeature.ORGAN_FAILURE, diafeature.CARDIOVASCULAR, diafeature.RENAL, diafeature.HEPATIC,
diafeature.HEMATOLOGIC, diafeature.METABOLIC, diafeature.NEUROLOGIC , vitals.HeartRate_Min , vitals.HeartRate_Max , vitals.HeartRate_Mean , vitals.SysBP_Min , vitals.HeartRate_Min, vitals.HeartRate_Max, vitals.HeartRate_Mean,
vitals.SysBP_Min, vitals.SysBP_Max, vitals.SysBP_Mean, vitals.DiasBP_Min, vitals.DiasBP_Max, vitals.DiasBP_Mean, vitals.MeanBP_Min, vitals.MeanBP_Max, vitals.MeanBP_Mean, vitals.RespRate_Min,
vitals.RespRate_Max, vitals.RespRate_Mean, vitals.TempC_Min, vitals.TempC_Max, vitals.TempC_Mean, vitals.SpO2_Min, vitals.SpO2_Max, vitals.SpO2_Mean, vitals.Glucose_Min, vitals.Glucose_Max, vitals.Glucose_Mean
FROM `physionet-data.mimiciii_clinical.admissions` AS adm
LEFT JOIN `physionet-data.mimiciii_clinical.patients` AS pts on adm.SUBJECT_ID = pts.SUBJECT_ID
LEFT JOIN icustay on adm.HADM_ID = icustay.HADM_ID
LEFT JOIN callout on adm.HADM_ID = callout.HADM_ID
LEFT JOIN diags on adm.HADM_ID = diags.HADM_ID
LEFT JOIN preps on adm.HADM_ID = preps.HADM_ID
LEFT JOIN procs on adm.HADM_ID = procs.HADM_ID
LEFT JOIN cpts on adm.HADM_ID = cpts.HADM_ID
LEFT JOIN labs on adm.HADM_ID = labs.HADM_ID
LEFT JOIN inputs_cv on adm.HADM_ID = inputs_cv.HADM_ID
LEFT JOIN inputs_mv on adm.HADM_ID = inputs_mv.HADM_ID
LEFT JOIN outputs on adm.HADM_ID = outputs.HADM_ID
LEFT JOIN transfers on adm.HADM_ID = transfers.HADM_ID
LEFT JOIN micros on adm.HADM_ID = micros.HADM_ID
LEFT JOIN diafeature on adm.HADM_ID = diafeature.HADM_ID
LEFT JOIN vitals on adm.HADM_ID = vitals.HADM_ID
