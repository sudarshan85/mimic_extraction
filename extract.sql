-- ------------------------------------------------------------------
-- Title: Gather data of patients between their hospital admission
--        and first ICU visit
-- MIMIC version: MIMIC-III v1.4
-- ------------------------------------------------------------------

drop materialized view if exists data cascade;
create materialized view data as

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

  -- time period between charttime and ICU intime
  , round((cast(extract(epoch from ai.intime- ae.charttime)/(60*60) as numeric)), 2) as chart_period

, case
  when ae.charttime between ai.intime - interval '12 hours' and ai.intime then 0
  when ae.charttime between ai.intime - interval '24 hours' and ai.intime - interval '12 hours' then
    1
  when ae.charttime between ai.intime - interval '36 hours' and ai.intime - interval '24 hours' then
    2
  when ae.charttime between ai.intime - interval '48 hours' and ai.intime - interval '36 hours' then
    3
  when ae.charttime between ai.intime - interval '60 hours' and ai.intime - interval '48 hours' then
    4
  when ae.charttime between ai.intime - interval '72 hours' and ai.intime - interval '60 hours' then
    5
  when ae.charttime between ai.intime - interval '84 hours' and ai.intime - interval '72 hours' then
    6
  when ae.charttime between ai.intime - interval '96 hours' and ai.intime - interval '84 hours' then
    7
  when ae.charttime between ai.intime - interval '108 hours' and ai.intime - interval '96 hours'
    then 8
  when ae.charttime between ai.intime - interval '120 hours' and ai.intime - interval '108 hours'
    then 9
  when ae.charttime between ai.intime - interval '132 hours' and ai.intime - interval '120 hours'
    then 10
  when ae.charttime between ai.intime - interval '144 hours' and ai.intime - interval '132 hours'
    then 11
  when ae.charttime between ai.intime - interval '156 hours' and ai.intime - interval '144 hours'
    then 12
  when ae.charttime between ai.intime - interval '168 hours' and ai.intime - interval '156 hours'
    then 13
  when ae.charttime between ai.intime - interval '180 hours' and ai.intime - interval '168 hours'
    then 14
  else 15 end as chartinterval

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
