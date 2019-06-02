-- ------------------------------------------------------------------
-- Description: Gather clinical notes for predicting first ICU visit
-- MIMIC version: MIMIC-III v1.4
-- ------------------------------------------------------------------

drop materialized view if exists timely_notes cascade;
create materialized view timely_notes as

with inter as
(
  select adm.hadm_id, adm.admittime, adm.dischtime
  , ie.icustay_id, ie.intime
  , pat.subject_id, pat.dob
  , ne.charttime
  , ne.category, ne.description, ne.text

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

  -- time period between hospital admission and its 1st icu visit in days 
  , round((cast(extract(epoch from ie.intime - adm.admittime)/(60*60*24) as numeric)), 2) as
  wait_period

  , round((cast(extract(epoch from ie.intime - ne.charttime)/(60*60*24) as numeric)), 2) as
  note_wait_time

  -- create labels for charttimes
  , case
    when ne.charttime between ie.intime - interval '1 day' and ie.intime then -1
    when ne.charttime between ie.intime - interval '3 days' and ie.intime - interval '1 day' then
      1
    when ne.charttime between ie.intime - interval '5 days' and ie.intime - interval '3 day' then
      -1
    else 0 end as class_label 

  from admissions adm
  inner join icustays ie on adm.hadm_id = ie.hadm_id
  inner join noteevents ne on adm.hadm_id = ne.hadm_id
  inner join patients pat on pat.subject_id = adm.subject_id
  where
  -- subjects should have recorded chartevents data
  adm.has_chartevents_data = 1 and
  -- discard subjects who have discharge time earlier than admittime
  adm.dischtime > adm.admittime and
  -- discard subjects who have ICU intime earlier than admittime
  ie.intime > adm.admittime and
  -- only include notes which are chartted between admittime and ICU intime
  ne.charttime between adm.admittime and ie.intime and
  -- discard documented erroneous notes
  ne.iserror is null 
)

select hadm_id, subject_id, icustay_id, admission_age, admittime, charttime, intime, wait_period
, note_wait_time
,category, description, text
, class_label

from inter
where
-- only include subjects with one admission or previous admission more than 30 days ago
include_adm = true and
-- only include subjects' first ICU visit for that admission
include_icu = true and
-- only include adult subjects
admission_age >= 15.0
order by hadm_id, icustay_id;
