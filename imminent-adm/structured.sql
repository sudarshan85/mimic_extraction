-- ------------------------------------------------------------------
-- MIMIC version: MIMIC-III v1.4
-- ------------------------------------------------------------------

drop materialized view if exists str cascade;
create materialized view str as

with inter as
(
  select adm.hadm_id
  , adm.admittime
  -- , adm.dischtime
  -- , adm.admission_type
  -- , adm.ethnicity
  -- , adm.deathtime
  , ie.intime
  -- , ie.los
  , pat.subject_id
  , pat.dob
  -- , pat.gender
  , ce.charttime as ce_charttime

  , case
      when dense_rank() over (partition by ie.hadm_id order by ie.intime) = 1 then true
      else false end as include_icu

  -- time period between hospital admission and its 1st icu visit in days 
  -- , round((cast(extract(epoch from intime - admittime)/(60*60*24) as numeric)), 2) as
  -- adm_to_icu

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
    -- sbp, dbp, map
    -- when itemid in (51,442,455,6701,220179,220050) and valuenum > 0 and valuenum < 400 then 2
    -- when itemid in (8368,8440,8441,8555,220180,220051) and valuenum > 0 and valuenum < 300 then 3 
    -- when itemid in (456,52,6702,443,220052,220181,225312) and valuenum > 0 and valuenum < 300 then 4
    -- -- resp
    -- when itemid in (615,618,220210,224690) and valuenum > 0 and valuenum < 70 then 5
    -- -- temp
    -- when itemid in (223761,678) and valuenum > 70 and valuenum < 120  then 6 -- F converted to C later
    -- when itemid in (223762,676) and valuenum > 10 and valuenum < 50  then 6 
    -- -- spo2
    -- when itemid in (646,220277) and valuenum > 0 and valuenum <= 100 then 7
    -- -- glucose
    -- when itemid in (807,811,1529,3745,3744,225664,220621,226537) and valuenum > 0 then 8
    -- -- base_excess
    -- when itemid in (74,776,3740,4196,224828) then 9
    -- -- hco3
    -- when itemid in (227443) then 10
    -- -- fio2 in both % and fraction
    -- when itemid in (185,186,189,190,3420,3421,3422,8517,223835) then 11 
    -- -- ph
    -- when itemid in (780,860,1126,1673,1880,3839,4202,4753,8387,220274,220734,223830) then 12
    -- -- paco2 in %
    -- when itemid in (778) then 13
    -- -- sa02 in %
    -- when itemid in (834,3495,3609,8532) then 14
    -- -- AST 
    -- when itemid in (770,220587) then 15
    -- -- BUN
    -- when itemid in (781,1162,3737,225624) then 16
    -- -- Alkaline phosphate
    -- when itemid in (773,3728,22561) then 17
    -- -- calcium
    -- when itemid in (786,816,1522,3746,225625,225667) then 18
    -- -- chloride
    -- when itemid in (788,1523,3747,220602,226536) then 19
    -- -- creatinine
    -- when itemid in (791,1525,3750,220615) then 20
    -- -- bilirubin direct
    -- when itemid in (225651) then 21
    -- -- bilirubin total
    -- when itemid in (225690) then 22
    -- -- lactic acid
    -- when itemid in (818,1531,225668) then 23
    -- -- magnesium
    -- when itemid in (821,1532,220635) then 24
    -- -- potassium
    -- when itemid in (829,1535,3792,227442,227464) then 25
    -- -- troponin
    -- when itemid in (851,227429) then 26
    -- -- hemotacrit
    -- when itemid in (813,3761,220545,226540) then 27
    -- -- hemoglobin
    -- when itemid in (814,3759,220228) then 28
    -- -- ptt
    -- when itemid in (825,1533,227466) then 29
    -- -- wbc
    -- when itemid in (861,1127,1542,4200,220546) then 30
    -- -- fibrinogen
    -- when itemid in (806,1528,227468) then 31
    -- -- platelets
    -- when itemid in (828) then 32
    else null end as var_id

  , valuenum

  from admissions adm
  inner join icustays ie on adm.hadm_id = ie.hadm_id and adm.has_chartevents_data = 1 and ie.intime > adm.admittime
  inner join chartevents ce on adm.hadm_id = ce.hadm_id and ce.charttime between adm.admittime and ie.intime and ce.error is distinct from 1
  inner join patients pat on adm.subject_id = pat.subject_id
  where ce.itemid in
  (
  -- HEART RATE
  211 --"Heart Rate"
  , 220045 --"Heart Rate"

  -- Systolic/diastolic
  -- , 51 --	Arterial BP [Systolic]
  -- , 442 --	Manual BP [Systolic]
  -- , 455 --	NBP [Systolic]
  -- , 6701 --	Arterial BP #2 [Systolic]
  -- , 220179 --	Non Invasive Blood Pressure systolic
  -- , 220050 --	Arterial Blood Pressure systolic

  -- , 8368 --	Arterial BP [Diastolic]
  -- , 8440 --	Manual BP [Diastolic]
  -- , 8441 --	NBP [Diastolic]
  -- , 8555 --	Arterial BP #2 [Diastolic]
  -- , 220180 --	Non Invasive Blood Pressure diastolic
  -- , 220051 --	Arterial Blood Pressure diastolic


  -- -- MEAN ARTERIAL PRESSURE
  -- , 52 --"Arterial BP Mean"
  -- , 443 --	Manual BP Mean(calc)
  -- , 456 --"NBP Mean"
  -- , 6702 --	Arterial BP Mean #2
  -- , 220052 --"Arterial Blood Pressure mean"
  -- , 220181 --"Non Invasive Blood Pressure mean"
  -- , 225312 --"ART BP mean"

  -- -- RESPIRATORY RATE
  -- , 615 --	Resp Rate (Total)
  -- , 618 --	Respiratory Rate
  -- , 220210 --	Respiratory Rate
  -- , 224690 --	Respiratory Rate (Total)

  -- -- TEMPERATURE
  -- , 676	-- "Temperature C"
  -- , 678 --	"Temperature F"
  -- , 223761 -- "Temperature Fahrenheit"
  -- , 223762 -- "Temperature Celsius"

  -- -- SPO2
  -- , 646 -- SpO2
  -- , 220277 -- O2 saturation pulseoxymetry

  -- -- GLUCOSE, both lab and fingerstick
  -- , 152 --	Glucose
  -- , 807 --	Fingerstick Glucose
  -- , 811--	Glucose (70-105)
  -- , 3744 --	Blood Glucose
  -- , 3745 --	BloodGlucose
  -- , 220621 --	Glucose (serum)
  -- , 226537 --	Glucose (whole blood)
  -- , 225664 --	Glucose finger stick

  -- -- Base Excess
  -- , 74 -- Base Excess
  -- , 776 -- Arterial Base Excess
  -- , 3740 -- Base Excess (other)
  -- , 4196 -- Base Excess (cap)
  -- , 224828 -- Arterial Base Excess

  -- -- hco3 
  -- , 227443 -- HCO3 (serum)

  -- -- FiO2
  -- , 185 -- FiO2 Alarm-High
  -- , 186 -- FiO2 Alarm-Low
  -- , 189 -- FiO2 (Analyzed)
  -- , 190 -- FiO2 Set
  -- , 3420 -- FiO2
  -- , 3421 -- FiO2 Alarm [Low]
  -- , 3422 -- FiO2 [Meas]
  -- , 8517 -- FiO2 Alarm [High]
  -- , 223835 -- Inspired O2 Fraction

  -- -- ph
  -- , 780 -- Arterial pH
  -- , 860 -- Venous pH
  -- , 1126 -- Art.pH
  -- , 1673 -- PH
  -- , 1880 -- Urine pH
  -- , 3839 -- ph (other)
  -- , 4202 -- ph (cap)
  -- , 4753 -- ph (Art)
  -- , 8387 -- GI [pH]
  -- , 220274 -- PH (Venous)
  -- , 220734 -- PH (dipstick)
  -- , 223830 -- PH (Arterial)

  -- -- Arterial PaCO2
  -- , 778

  -- -- SaO2
  -- , 834 -- SaO2
  -- , 3495 -- Lowest SaO2
  -- , 3609 -- SaO2 Alarm [Low]
  -- , 8532 -- SaO2 Alarm [High]

  -- -- AST
  -- , 770 -- carevue
  -- , 220587 -- metavision

  -- -- BUN
  -- , 781
  -- , 1162
  -- , 3737
  -- , 225624

  -- -- Alkaline phosphate
  -- , 773 -- Alk. Phosphate
  -- , 3728 -- Alkaline Phosphatase
  -- , 22561 -- Alkaline phosphate

  -- -- calcium
  -- , 786 -- Calcium (8.4-10.2)
  -- , 816 -- Inonized Calcium
  -- , 1522 -- Calcium
  -- , 3746 -- Calcium (8.8-10.8)
  -- , 225625 -- Calcium non-ionized
  -- , 225667 -- Ionized Calcium

  -- -- chloride
  -- , 788 -- Chloride (100-112)
  -- , 1523 -- Chloride
  -- , 3747 -- Chloride (100-112)
  -- , 220602 -- Chloride (serum)
  -- , 226536 -- Chloride (whole blood)

  -- -- creatinine
  -- , 791 -- Creatinine (0-1.3)
  -- , 1525 -- Creatinine
  -- , 3750 -- Creatinine (0-0.7)
  -- , 220615 -- Creatinine

  -- -- bilirubin
  -- , 225651 -- Direct Bilirubin
  -- , 225690 -- Total Bilirubin

  -- -- lactic acid
  -- , 818 -- Lactic Acid (0.5-2.0)
  -- , 1531 -- Lactic Acid
  -- , 225668 -- Lactic Acid

  -- -- magnesium
  -- , 821 -- Magnesium (1.6-2.6)
  -- , 1532 -- Magnesium
  -- , 220635 -- Magnesium

  -- -- potassium
  -- , 829 -- Potassium (3.5-5.3)
  -- , 1535 -- Potassium 
  -- , 3792 -- Potassium (3.5-5.3)
  -- , 227442 -- Potassium (serum)
  -- , 227464 -- Potassium (whole blood)

  -- -- troponin
  -- , 851 -- Troponin
  -- , 227429 -- Troponin-T

  -- -- hematocrit
  -- , 813 -- Hematocrit
  -- , 3761 -- Hematocrit (35-51)
  -- , 220545 -- Hematocrit (serum)
  -- , 226540 -- Hematocrit (whole blood -calc)

  -- -- hemoglobin
  -- , 814 -- Hemoglobin
  -- , 3759 -- HGB (10.8-15.8)
  -- , 220228 -- Hemoglobin

  -- -- ptt
  -- , 825 -- PTT (22-35)
  -- , 1533 -- PTT
  -- , 227466 -- PTT

  -- -- wbc
  -- , 861 -- WBC (4-11,000)
  -- , 1127 -- WBC (4-11,000)
  -- , 1542 -- WBC
  -- , 4200 -- WBC 4.0-11.0
  -- , 220546 -- WBC

  -- -- fibrinogen
  -- , 806 -- Fibrinogen (150-400)
  -- , 1528 -- Fibrinogen
  -- , 227468 -- Fibrinogen

  -- -- platelets
  -- , 828 -- Platelets

)
)

select hadm_id
 -- , subject_id
 -- , admission_type
 -- , intime
-- , adm_to_icu
 , ce_charttime
 -- , los as icu_los
 -- , deathtime
 -- , ethnicity
 -- , dob
 -- , gender 

  -- time period between structured data charttime and 1st icu visit in days
  -- , round((cast(extract(epoch from intime - ce_charttime)/(60*60*24) as numeric)), 2) as
  -- ce_charttime_to_icu

-- , case
  -- when ce_charttime between intime - interval '1 day' and intime then 0
  -- when ce_charttime between intime - interval '2 days' and intime - interval '1 day' then 1
  -- when ce_charttime between intime - interval '3 days' and intime - interval '2 days' then 2
  -- when ce_charttime between intime - interval '4 days' and intime - interval '3 days' then 3
  -- when ce_charttime between intime - interval '5 days' and intime - interval '4 days' then 4
  -- when ce_charttime between intime - interval '6 days' and intime - interval '5 days' then 5
  -- when ce_charttime between intime - interval '7 days' and intime - interval '6 days' then 6
  -- when ce_charttime between intime - interval '8 days' and intime - interval '7 days' then 7
  -- when ce_charttime between intime - interval '9 days' and intime - interval '8 days' then 8
  -- when ce_charttime between intime - interval '10 days' and intime - interval '9 days' then 9
  -- when ce_charttime between intime - interval '11 days' and intime - interval '10 days' then
    -- 10 
  -- when ce_charttime between intime - interval '12 days' and intime - interval '11 days' then
    -- 11 
  -- when ce_charttime between intime - interval '13 days' and intime - interval '12 days' then
    -- 12 
  -- when ce_charttime between intime - interval '14 days' and intime - interval '13 days' then
    -- 13 
  -- when ce_charttime between intime - interval '15 days' and intime - interval '14 days' then
    -- 14 
  -- else 15 end as ce_chartinterval

-- Easier names
, case when var_id = 1 then valuenum else null end as hr
-- , case when var_id = 2 then valuenum else null end as sbp
-- , case when var_id = 3 then valuenum else null end as dbp 
-- , case when var_id = 4 then valuenum else null end as map
-- , case when var_id = 5 then valuenum else null end as resp

-- -- convert F to C
-- , case
  -- when var_id = 6 and valuenum < 50 then valuenum
  -- when var_id = 6 and valuenum > 70 then (valuenum-32)/1.8
  -- else null end as temp

-- , case when var_id = 7 then valuenum else null end as spo2
-- , case when var_id = 8 then valuenum else null end as glucose
-- , case when var_id = 9 then valuenum else null end as base_excess
-- , case when var_id = 10 then valuenum else null end as hco3

-- -- convert fio2 % to fraction
-- , case
  -- when var_id = 11 and valuenum > 100 then 1
  -- when var_id = 11 and valuenum <= 1 then valuenum
  -- when var_id = 11 and valuenum > 1 then valuenum/100
  -- else null end as fio2

-- , case when var_id = 12 then valuenum else null end as ph

-- -- convert paco2 % to fraction
-- , case
  -- when var_id = 13 and valuenum > 100 then 1
  -- when var_id = 13 and valuenum <= 1 then valuenum
  -- when var_id = 13 and valuenum > 1 then valuenum/100
  -- else null end as paco2

-- -- convert sao2% to fraction
-- , case
  -- when var_id = 14 and valuenum > 100 then 1
  -- when var_id = 14 and valuenum <= 1 then valuenum
  -- when var_id = 14 and valuenum > 1 then valuenum/100
  -- else null end as sao2

-- , case when var_id = 15 then valuenum else null end as ast
-- , case when var_id = 16 then valuenum else null end as bun
-- , case when var_id = 17 then valuenum else null end as alp
-- , case when var_id = 18 then valuenum else null end as calcium
-- , case when var_id = 19 then valuenum else null end as chloride
-- , case when var_id = 20 then valuenum else null end as creatinine
-- , case when var_id = 21 then valuenum else null end as bilirubin_direct
-- , case when var_id = 22 then valuenum else null end as bilirubin_total
-- , case when var_id = 23 then valuenum else null end as lactic_acid
-- , case when var_id = 24 then valuenum else null end as magnesium
-- , case when var_id = 25 then valuenum else null end as potassium
-- , case when var_id = 26 then valuenum else null end as troponin
-- , case when var_id = 27 then valuenum else null end as hematocrit
-- , case when var_id = 28 then valuenum else null end as hemoglobin
-- , case when var_id = 29 then valuenum else null end as ptt
-- , case when var_id = 30 then valuenum else null end as wbc
-- , case when var_id = 31 then valuenum else null end as fibrinogen
-- , case when var_id = 32 then valuenum else null end as platelets

from inter
where include_adm = true -- only include subjects with one admission or previous admission more than 30 days ago
and include_icu = true -- only include subjects' first ICU visit for that admission
and admission_age >= 15.0 -- only include adult subjects
order by hadm_id;
