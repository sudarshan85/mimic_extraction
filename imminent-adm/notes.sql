-- ------------------------------------------------------------------
-- MIMIC version: MIMIC-III v1.4
-- ------------------------------------------------------------------

drop materialized view if exists notes cascade;
create materialized view notes as

with inter as
(
  select adm.hadm_id
  , adm.admittime
  , adm.dischtime 
  , adm.admission_type
  , adm.ethnicity
  , adm.deathtime
  , ie.intime
  , ie.los
  , pat.subject_id
  , pat.dob
  , pat.gender
  , ne.charttime as ne_charttime
  , ne.category
  , ne.description
  , ne.text

  , case
      when dense_rank() over (partition by ie.hadm_id order by ie.intime) = 1 then true
      else false end as include_icu

  -- time period between hospital admission and its 1st icu visit in days 
  , round((cast(extract(epoch from intime - admittime)/(60*60*24) as numeric)), 2) as
  adm_to_icu

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

  from admissions adm
  inner join icustays ie on adm.hadm_id = ie.hadm_id and adm.has_chartevents_data = 1 and ie.intime > adm.admittime
  inner join noteevents ne on adm.hadm_id = ne.hadm_id and ne.charttime between adm.admittime and ie.intime and ne.iserror is null
  inner join patients pat on pat.subject_id = adm.subject_id
)

select hadm_id
, subject_id
, admission_type
, intime
, adm_to_icu
, ne_charttime
, los as icu_los
, deathtime
, ethnicity
, dob
, gender

, round((cast(extract(epoch from intime - ne_charttime)/(60*60*24) as numeric)), 2) as
ne_charttime_to_icu

, case
  when ne_charttime between intime - interval '1 day' and intime then 0
  when ne_charttime between intime - interval '2 days' and intime - interval '1 day' then 1
  when ne_charttime between intime - interval '3 days' and intime - interval '2 days' then 2
  when ne_charttime between intime - interval '4 days' and intime - interval '3 days' then 3
  when ne_charttime between intime - interval '5 days' and intime - interval '4 days' then 4
  when ne_charttime between intime - interval '6 days' and intime - interval '5 days' then 5
  when ne_charttime between intime - interval '7 days' and intime - interval '6 days' then 6
  when ne_charttime between intime - interval '8 days' and intime - interval '7 days' then 7
  when ne_charttime between intime - interval '9 days' and intime - interval '8 days' then 8
  when ne_charttime between intime - interval '10 days' and intime - interval '9 days' then 9
  when ne_charttime between intime - interval '11 days' and intime - interval '10 days' then
    10 
  when ne_charttime between intime - interval '12 days' and intime - interval '11 days' then
    11 
  when ne_charttime between intime - interval '13 days' and intime - interval '12 days' then
    12 
  when ne_charttime between intime - interval '14 days' and intime - interval '13 days' then
    13 
  when ne_charttime between intime - interval '15 days' and intime - interval '14 days' then
    14 
  else 15 end as ne_chartinterval

, category
, description
, text

from inter
where include_adm = true -- only include subjects with one admission or previous admission more than 30 days ago
and include_icu = true -- only include subjects' first ICU visit for that admission
and admission_age >= 15.0 -- only include adult subjects
order by hadm_id;

