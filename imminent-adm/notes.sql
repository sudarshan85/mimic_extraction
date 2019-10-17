-- ------------------------------------------------------------------
-- MIMIC version: MIMIC-III v1.4
-- ------------------------------------------------------------------

drop materialized view if exists notes cascade;
create materialized view notes as

with inter as
(
  select adm.hadm_id
  , adm.admittime
  , ie.intime
  , ie.los
  , pat.subject_id
  , pat.dob
  , ne.charttime as ne_charttime
  , ne.category
  , ne.description
  , ne.text

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

  from admissions adm
  inner join icustays ie on adm.hadm_id = ie.hadm_id and adm.has_chartevents_data = 1 and ie.intime > adm.admittime
  inner join noteevents ne on adm.hadm_id = ne.hadm_id and ne.charttime between adm.admittime and ie.intime and ne.iserror is null
  inner join patients pat on pat.subject_id = adm.subject_id
)

select hadm_id
, intime
, admittime
, ne_charttime
, category
, description
, text

from inter
where include_adm = true -- only include subjects with one admission or previous admission more than 30 days ago
and include_icu = true -- only include subjects' first ICU visit for that admission
and admission_age >= 15.0 -- only include adult subjects
order by hadm_id;

