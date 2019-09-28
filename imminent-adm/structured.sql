-- ------------------------------------------------------------------
-- MIMIC version: MIMIC-III v1.4
-- ------------------------------------------------------------------

drop materialized view if exists str cascade;
create materialized view str as

with inter as
(
  select adm.hadm_id
  , adm.admittime
  , adm.dischtime
  , ie.subject_id
  , ie.icustay_id
  , ce.charttime
  , ie.intime
  , pat.dob
  , pat.gender

  , case
      when dense_rank() over (partition by ie.hadm_id order by ie.intime) = 1 then true
      else false end as include_icu

  , case
  -- mark the first hospital adm 
      when dense_rank() over (partition by adm.subject_id order by adm.admittime) = 1 then true
  -- mark subsequent hospital adms if its been atleast a month since previous admission.
  -- Defined using lag() as shown here: http://bit.ly/2KpJaeg
      when round((cast(extract(epoch from adm.admittime - lag(adm.admittime, 1) over (partition by
        adm.subject_id order by adm.admittime))/(60*60*24) as numeric)), 2) >= 30.0 then true
      else false end as include_adm

  , round((cast(extract(epoch from adm.admittime - pat.dob)/(60*60*24*365.242) as numeric)), 2) as
  admission_age

  , case
    -- hr
    when itemid in (211,220045) and valuenum > 0 and valuenum < 300 then 1 
    sbp, dbp, map
    when itemid in (51,442,455,6701,220179,220050) and valuenum > 0 and valuenum < 400 then 2
    when itemid in (8368,8440,8441,8555,220180,220051) and valuenum > 0 and valuenum < 300 then 3 
    when itemid in (456,52,6702,443,220052,220181,225312) and valuenum > 0 and valuenum < 300 then 4
    resp
    when itemid in (615,618,220210,224690) and valuenum > 0 and valuenum < 70 then 5
    temp
    when itemid in (223761,678) and valuenum > 70 and valuenum < 120  then 6 F converted to C in valuenum call
    when itemid in (223762,676) and valuenum > 10 and valuenum < 50  then 6 
    spo2
    when itemid in (646,220277) and valuenum > 0 and valuenum <= 100 then 7
    glucose
    when itemid in (807,811,1529,3745,3744,225664,220621,226537) and valuenum > 0 then 8
    base_excess
    when itemid in (74,776,3740,4196,224828) and valuenum >= -20 and valuenum <= 20 then 9
    hco3
    when itemid in (227443) and valuenum <= 50 then 10
    fio2 in both % and fraction
    when itemid in (185,186,189,190,3420,3421,3422,8517,223835) then 11 
    ph
    when itemid in (780,860,1126,1673,1880,3839,4202,4753,8387,220274,220734,223830) and valuenum >= 0 and valuenum < 20 then 12
    paco2 in %
    when itemid in (778) then 13
    sa02 in %
    when itemid in (834,3495,3609,8532) and valuenum >= 0 and valuenum <= 100 then 14
    else null end as var_id
      

  , valuenum

  from admissions adm
  inner join icustays ie on adm.hadm_id = ie.hadm_id
  inner join patients pat on pat.subject_id = adm.subject_id
  left join chartevents ce
  on ie.subject_id = ce.subject_id and ie.hadm_id = ce.hadm_id and ie.icustay_id = ce.icustay_id
  and ce.charttime between adm.admittime and ie.intime
  and adm.dischtime > adm.admittime
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
  678, --	"Temperature F"

  -- Base Excess
  74, -- Base Excess
  776, -- Arterial Base Excess
  3740, -- Base Excess (other)
  3829, -- Venous Base Excess
  4196, -- Base Excess (cap)
  224828, -- Arterial Base Excess

  -- HCO3 (serum)
  227443,

  -- FiO2
  185, -- FiO2 Alarm-High
  186, -- FiO2 Alarm-Low
  189, -- FiO2 (Analyzed)
  190, -- FiO2 Set
  3420, -- FiO2
  3421, -- FiO2 Alarm [Low]
  3422, -- FiO2 [Meas]
  8517, -- FiO2 Alarm [High]
  223835, -- Inspired O2 Fraction

  -- ph
  780, -- Arterial pH
  860, -- Venous pH
  1126, -- Art.pH
  1673, -- PH
  1880, -- Urine pH
  3839, -- ph (other)
  4202, -- ph (cap)
  4753, -- ph (Art)
  8387, -- GI [pH]
  220274, -- PH (Venous)
  220734, -- PH (dipstick)
  223830, -- PH (Arterial)

  -- Arterial PaCO2
  778,

  -- SaO2
  834, -- SaO2
  3495, -- Lowest SaO2
  3609, -- SaO2 Alarm [Low]
  8532 -- SaO2 Alarm [High]
)
)

SELECT subject_id, hadm_id, icustay_id, dob, gender, admittime, intime, charttime

-- Easier names
, case when var_id = 1 then valuenum else null end as hr
, case when var_id = 2 then valuenum else null end as sbp
, case when var_id = 3 then valuenum else null end as dbp 
, case when var_id = 4 then valuenum else null end as map
, case when var_id = 5 then valuenum else null end as resp

-- convert F to C
, case
  when var_id = 6 and valuenum < 50 then valuenum
  when var_id = 6 and valuenum > 70 then (valuenum-32)/1.8
  else null end as temp

, case when var_id = 7 then valuenum else null end as spo2
, case when var_id = 8 then valuenum else null end as glucose
, case when var_id = 9 then valuenum else null end as base_excess
, case when var_id = 10 then valuenum else null end as hco3

-- convert fio2 % to fraction
, case
  when var_id = 11 and valuenum > 100 then 1
  when var_id = 11 and valuenum <= 1 then valuenum
  when var_id = 11 and valuenum > 1 then valuenum/100
  else null end as fio2

, case when var_id = 12 then valuenum else null end as ph

-- convert paco2 % to fraction
, case
  when var_id = 13 then valuenum/100
  else null end as paco2 

-- convert sao2 % to fraction
, case
  when var_id = 14 then valuenum/100
  else null end as sao2

from inter
where include_adm = true
and include_icu = true
order by hadm_id, icustay_id;
