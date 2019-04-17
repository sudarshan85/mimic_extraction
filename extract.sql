-- ------------------------------------------------------------------
-- Title: Gather data of patients between their hospital admission
--        and first ICU visit
-- MIMIC version: MIMIC-III v1.4
-- ------------------------------------------------------------------

drop materialized view if exists myco cascade;
create materialized view myco as

-- Grab data from icustays table joining on hadm_id
with admicu as 
(
  select adm.hadm_id, adm.subject_id, adm.admittime, adm.dischtime, adm.has_chartevents_data
  , icu.icustay_id, icu.intime

  -- time period between hospital admission and its 1st icu visit in hours
  , round((cast(extract(epoch from icu.intime - adm.admittime)/(60*60) as numeric)), 2) as
  wait_period

  -- los hospital in hours
  , round((cast(extract(epoch from adm.dischtime - adm.admittime)/(60*60) as numeric)), 2) as
  los_hospital

  , case
  -- mark the first hospital adm 
      when dense_rank() over (partition by adm.subject_id order by adm.admittime) = 1 then true
  -- mark subsequent hospital adms if its been atleast a month since previous admission.
  -- Defined using lag() as shown here: http://bit.ly/2KpJaeg
      when round((cast(extract(epoch from adm.admittime - lag(adm.admittime, 1) over (partition by
        adm.subject_id order by adm.admittime))/(60*60*24) as numeric)), 2) >= 30.0 then true
      else false end as include_adm

  -- mark the first icu stay for current hospital admission
  , case
      when dense_rank() over (partition by icu.hadm_id order by icu.intime) = 1 then true
      else false end as include_icu

  from admissions adm
  inner join icustays icu
    on icu.hadm_id = adm.hadm_id
), admne as
(
  select adm.hadm_id, ne.charttime, ne.iserror
  , ne.category, ne.description, ne.text

  from admissions adm
  inner join noteevents ne
    on ne.hadm_id = adm.hadm_id
), age as
(
  select pat.subject_id, pat.dob

  -- age at admission in years
  , round((cast(extract(epoch from adm.admittime - pat.dob)/(60*60*24*365.242) as numeric)), 2) as
  admission_age

  from patients pat
  inner join admissions adm
    on adm.subject_id = pat.subject_id
)

select ai.hadm_id, ai.subject_id, ai.icustay_id, age.admission_age, ai.admittime, ai.dischtime
, ai.los_hospital, ae.charttime, ai.intime, ai.wait_period
, ae.category, ae.description, ae.text

from admicu ai
inner join admne ae
  on ae.hadm_id = ai.hadm_id
inner join age
  on age.subject_id = ai.subject_id
where
-- subjects should have recorded chartevents data
ai.has_chartevents_data = 1 and
-- discard subjects who have discharge time earlier than admittime
ai.dischtime > ai.admittime and
-- discard subjects who have ICU intime earlier than admittime
ai.intime > ai.admittime and
-- only include subjects with one admission or previous admission more than 30 days ago
ai.include_adm = true and
-- only include subjects' first ICU visit for that admission
ai.include_icu = true and
-- only include adult subjects
age.admission_age >= 15.0 and
-- only include notes which are chartted between admittime and ICU intime
ae.charttime between admittime and intime and
-- discard documented erroneous notes
ae.iserror is null 
order by ai.hadm_id, ai.intime;
