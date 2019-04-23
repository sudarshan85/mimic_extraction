-- ------------------------------------------------------------------
-- Description: Gather clinical notes for predicting first ICU visit
-- MIMIC version: MIMIC-III v1.4
-- ------------------------------------------------------------------

drop materialized view if exists notes cascade;
create materialized view notes as

with admne as
(
  select adm.hadm_id, ne.charttime
  , ne.category, ne.description, ne.text

  from admissions adm
  inner join noteevents ne
    on ne.hadm_id = adm.hadm_id
  where
  ne.iserror is null and
  ne.charttime is not null
)

select ie.hadm_id, ie.subject_id, ie.icustay_id, ae.charttime, ie.intime
, ae.category, ae.description, ae.text

, round((cast(extract(epoch from ie.intime - ae.charttime)/(60*60*24) as numeric)), 2) as
note_wait_time

, case
  when ae.charttime between ie.intime - interval '1 day' and ie.intime then 0
  when ae.charttime between ie.intime - interval '2 days' and ie.intime - interval '1 day' then 1
  when ae.charttime between ie.intime - interval '3 days' and ie.intime - interval '2 days' then 2
  when ae.charttime between ie.intime - interval '4 days' and ie.intime - interval '3 days' then 3
  when ae.charttime between ie.intime - interval '5 days' and ie.intime - interval '4 days' then 4
  when ae.charttime between ie.intime - interval '6 days' and ie.intime - interval '5 days' then 5
  when ae.charttime between ie.intime - interval '7 days' and ie.intime - interval '6 days' then 6
  when ae.charttime between ie.intime - interval '8 days' and ie.intime - interval '7 days' then 7
  when ae.charttime between ie.intime - interval '9 days' and ie.intime - interval '8 days' then 8
  when ae.charttime between ie.intime - interval '10 days' and ie.intime - interval '9 days' then 9
  when ae.charttime between ie.intime - interval '11 days' and ie.intime - interval '10 days' then
    10 
  when ae.charttime between ie.intime - interval '12 days' and ie.intime - interval '11 days' then
    11 
  when ae.charttime between ie.intime - interval '13 days' and ie.intime - interval '12 days' then
    12 
  when ae.charttime between ie.intime - interval '14 days' and ie.intime - interval '13 days' then
    13 
  when ae.charttime between ie.intime - interval '15 days' and ie.intime - interval '14 days' then
    14 
  else 15 end as chartinterval

from icustays ie
inner join admne ae
  on ae.hadm_id = ie.hadm_id
where
ae.charttime <= ie.intime;
