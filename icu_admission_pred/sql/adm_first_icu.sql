-- ------------------------------------------------------------------
-- Title: Gather data of patients between their hospital admission
--        and first ICU visit
-- MIMIC version: MIMIC-III v1.4
-- ------------------------------------------------------------------

drop materialized view if exists co cascade;
create materialized view co as

-- Grab data from icustays table joining on hadm_id
with admicu as 
(
  select adm.hadm_id, adm.admittime
  , icu.icustay_id, icu.intime

  -- time period between hospital admission and its 1st icu visit in hours
  , round((cast(extract(epoch from icu.intime - adm.admittime)/(60*60) as numeric)), 2) as
  wait_period

  , case
  -- mark the first hospital adm 
      when dense_rank() over (partition by adm.subject_id order by adm.admittime) = 1 then true
  -- mark subsequent hospital adms if its been atleast a month since previous admission.
  -- Defined using lag() as shown here: http://bit.ly/2KpJaeg
      when round((cast(extract(epoch from adm.admittime - lag(adm.admittime, 1) over (partition by
        adm.subject_id order by adm.admittime))/(60*60*24) as numeric)), 2) > 30.0 then true
      else false end as include_adm

  -- mark the first icu stay for current hospital admission
  , case
      when dense_rank() over (partition by icu.hadm_id order by icu.intime) = 1 then true
      else false end as include_icu

  from admissions adm
  inner join icustays icu
    on icu.hadm_id = adm.hadm_id
)
-- Grab data from the patients table joining on subject_id
, patadm as
(
  select pat.subject_id, pat.dob, pat.dod
  , adm.hadm_id, adm.admittime, adm.dischtime, adm.admission_type, adm.admission_location
  , adm.insurance, adm.language, adm.religion, adm.marital_status, adm.ethnicity, adm.diagnosis

  -- age at admission in years
  , round((cast(extract(epoch from adm.admittime - pat.dob)/(60*60*24*365.242) as numeric)), 2) as
  admission_age

  -- los hospital in hours
  , round((cast(extract(epoch from adm.dischtime - adm.admittime)/(60*60) as numeric)), 2) as
  los_hospital

  from patients pat
  inner join admissions adm
    on adm.subject_id = pat.subject_id
  where adm.has_chartevents_data = 1
)
-- Grab echodata and notes data by first join both using row_id as specified here:
-- http://bit.ly/2Zh3phU
, notes as
(
  select ne.row_id, ne.subject_id, ne.hadm_id, ne.chartdate, ne.charttime, ne.storetime,
  ne.category, ne.description, ne.cgid, ne.iserror, ne.text

  from noteevents ne
  where ne.iserror is null
)

select pa.subject_id
, pa.hadm_id, ai.icustay_id, pa.admission_age, pa.admittime, ai.intime, en.charttime
, ai.wait_period, pa.los_hospital, pa.admission_type, pa.admission_location, pa.insurance
, pa.language, pa.religion, pa.marital_status, pa.ethnicity, pa.diagnosis
, en.category, en.description, en.text
, ai.include_adm, ai.include_icu

from patadm pa
inner join admicu ai
  on ai.hadm_id = pa.hadm_id
inner join notes en
  on en.hadm_id = pa.hadm_id
order by pa.subject_id, pa.admittime;
