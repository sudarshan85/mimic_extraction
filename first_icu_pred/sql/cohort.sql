-- ------------------------------------------------------------------
-- Description: Gather clinical notes for predicting first ICU visit
-- MIMIC version: MIMIC-III v1.4
-- ------------------------------------------------------------------

drop materialized view if exists co cascade;
create materialized view co as

with inter as
(
  select adm.hadm_id, adm.admittime, adm.dischtime, adm.admission_type, adm.ethnicity
  , pat.subject_id, pat.dob, pat.gender
  , ie.icustay_id, ie.intime
  , ne.charttime, ne.category, ne.description, ne.text

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
  adm_icu_period

  , round((cast(extract(epoch from ie.intime - ne.charttime)/(60*60*24) as numeric)), 2) as
  chart_icu_period

  , case
    when ne.charttime between ie.intime - interval '1 day' and ie.intime then 0
    when ne.charttime between ie.intime - interval '2 days' and ie.intime - interval '1 day' then 1
    when ne.charttime between ie.intime - interval '3 days' and ie.intime - interval '2 days' then 2
    when ne.charttime between ie.intime - interval '4 days' and ie.intime - interval '3 days' then 3
    when ne.charttime between ie.intime - interval '5 days' and ie.intime - interval '4 days' then 4
    when ne.charttime between ie.intime - interval '6 days' and ie.intime - interval '5 days' then 5
    when ne.charttime between ie.intime - interval '7 days' and ie.intime - interval '6 days' then 6
    when ne.charttime between ie.intime - interval '8 days' and ie.intime - interval '7 days' then 7
    when ne.charttime between ie.intime - interval '9 days' and ie.intime - interval '8 days' then 8
    when ne.charttime between ie.intime - interval '10 days' and ie.intime - interval '9 days' then 9
    when ne.charttime between ie.intime - interval '11 days' and ie.intime - interval '10 days' then
      10 
    when ne.charttime between ie.intime - interval '12 days' and ie.intime - interval '11 days' then
      11 
    when ne.charttime between ie.intime - interval '13 days' and ie.intime - interval '12 days' then
      12 
    when ne.charttime between ie.intime - interval '14 days' and ie.intime - interval '13 days' then
      13 
    when ne.charttime between ie.intime - interval '15 days' and ie.intime - interval '14 days' then
      14 
    else 15 end as chartinterval

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

select subject_id, hadm_id, icustay_id, admission_type
, admittime, dischtime, intime, charttime
, adm_icu_period, chart_icu_period, chartinterval
, ethnicity, dob, gender, admission_age
, category, description, text
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
