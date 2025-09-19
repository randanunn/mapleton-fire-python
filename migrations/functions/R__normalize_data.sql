drop function if exists normalize_data();
CREATE OR REPLACE FUNCTION normalize_data()
  RETURNS void AS
$$
BEGIN
  SET TIME ZONE 'America/Denver';
-- enable once per database
  CREATE EXTENSION IF NOT EXISTS tablefunc;

--update all date time stuff to usable dates in pg
  alter table sheet_data add column if not exists call_psap_time timestamp;
  alter table sheet_data add column if not exists call_dispatched_time timestamp;
  alter table sheet_data add column if not exists call_enroute_time timestamp;
  alter table sheet_data add column if not exists call_arrived_time timestamp;
  alter table sheet_data add column if not exists call_complete_time timestamp;
  update sheet_data set
                      call_psap_time = case when psap_time_hhmmss_please_enter_seconds_for_data_purposes is not null then concat(incident_date_mmddyyyy::date, ' ',       trim(psap_time_hhmmss_please_enter_seconds_for_data_purposes))::timestamp else null end,
                      call_dispatched_time = case when disp_time_hhmmss_please_enter_seconds_for_data_purposes is not null then concat(incident_date_mmddyyyy::date, ' ',       trim(disp_time_hhmmss_please_enter_seconds_for_data_purposes))::timestamp else null end,
                      call_enroute_time = case when enrt_time_1st_mafd_fire_apparatus_hhmmss_please_enter_seconds_f is not null then concat(incident_date_mmddyyyy::date, ' ',  trim(enrt_time_1st_mafd_fire_apparatus_hhmmss_please_enter_seconds_f))::timestamp else null end,
                      call_arrived_time = case when arrvd_time_1st_mafd_fire_apparatus_hhmmss_please_enter_seconds_ is not null then concat(incident_date_mmddyyyy::date, ' ',  trim(arrvd_time_1st_mafd_fire_apparatus_hhmmss_please_enter_seconds_))::timestamp else null end,
                      call_complete_time = case when cmplt_time_1st_mafd_fire_apparatus_hhmmss_please_enter_seconds_ is not null then concat(incident_date_mmddyyyy::date, ' ', trim(cmplt_time_1st_mafd_fire_apparatus_hhmmss_please_enter_seconds_))::timestamp else null end
  ;

--add a day to end if it crossed over
  update sheet_data
  set call_dispatched_time = call_dispatched_time + interval '1 day'
  where call_psap_time > call_dispatched_time;
  update sheet_data
  set call_enroute_time = call_enroute_time + interval '1 day'
  where call_dispatched_time > call_enroute_time;
  update sheet_data
  set call_arrived_time = call_arrived_time + interval '1 day'
  where call_dispatched_time > call_arrived_time;
  update sheet_data
  set call_complete_time = call_complete_time + interval '1 day'
  where call_dispatched_time > call_complete_time;

--update response time so i can just update it here and not have to update queries later
  alter table sheet_data drop column if exists response_time;
  alter table sheet_data add column if not exists response_time interval;
  update sheet_data set response_time = call_arrived_time - call_dispatched_time;
  alter table sheet_data drop column if exists response_time_seconds;
  alter table sheet_data add column if not exists response_time_seconds bigint;
  update sheet_data set response_time_seconds = EXTRACT(EPOCH FROM (call_arrived_time - call_dispatched_time))::bigint;

--update city
  update sheet_data
  set city = 'UTAH COUNTY'
  where city != 'UTAH COUNTY'
    and city like '%UTAH COUNTY%'
  ;
--get a normal list of quadrants
  alter table sheet_data drop column if exists quadrant_normalize;
  alter table sheet_data add column if not exists quadrant_normalize text;
  alter table sheet_data drop column if exists quadrant_sort;
  alter table sheet_data add column if not exists quadrant_sort bigint;
  update sheet_data set quadrant_normalize = 'NORTH WEST', quadrant_sort = 1 where area_quadrant_only_for_mapleton_city like 'NORTH WEST%';
  update sheet_data set quadrant_normalize = 'NORTH EAST', quadrant_sort = 2 where area_quadrant_only_for_mapleton_city like 'NORTH EAST%';
  update sheet_data set quadrant_normalize = 'SOUTH WEST', quadrant_sort = 3 where area_quadrant_only_for_mapleton_city like 'SOUTH WEST%';
  update sheet_data set quadrant_normalize = 'SOUTH EAST', quadrant_sort = 4 where area_quadrant_only_for_mapleton_city like 'SOUTH EAST%';
  update sheet_data set quadrant_normalize = area_quadrant_only_for_mapleton_city, quadrant_sort = 5 where quadrant_normalize is null;
  update sheet_data set quadrant_normalize = 'OTHER', quadrant_sort = 6 where quadrant_normalize is null OR quadrant_normalize = '';

--do some overlap counting helper stuff
  alter table sheet_data drop column if exists has_overlap;
  alter table sheet_data add column if not exists has_overlap bool not null default false;
  update sheet_data t1
  set has_overlap = true
  where exists (
    select id from sheet_data t2
    where t1.id != t2.id
      and
      (t1.call_dispatched_time between t2.call_dispatched_time and t2.call_complete_time
        or
       t1.call_complete_time between t2.call_dispatched_time and t2.call_complete_time
        or
       t2.call_dispatched_time between t1.call_dispatched_time and t1.call_complete_time
        or
       t2.call_complete_time between t1.call_dispatched_time and t1.call_complete_time
        ));

--only count an overlap once if it overlaps the previous row (the other way is double and triple counting)
  alter table sheet_data drop column if exists overlap_previous;
  alter table sheet_data add column if not exists overlap_previous bool not null default false;
  update sheet_data t1
  set overlap_previous = true
  where exists (
    select id from sheet_data t2
--   where t2.id = (t1.id + 1)
    where t2.id + 1 = t1.id
      and
      (t1.call_dispatched_time between t2.call_dispatched_time and t2.call_complete_time
        or
       t1.call_complete_time between t2.call_dispatched_time and t2.call_complete_time
        or
       t2.call_dispatched_time between t1.call_dispatched_time and t1.call_complete_time
        or
       t2.call_complete_time between t1.call_dispatched_time and t1.call_complete_time
        ));

--   this prevents supabase.js from running selects. it can only do rpc calls which call functions and those have pre-defined data set returns
  ALTER TABLE sheet_data ENABLE ROW LEVEL SECURITY;
  CREATE POLICY "deny all for anon"
    ON sheet_data
    FOR SELECT
    TO anon
    USING (false);
END;
$$ LANGUAGE plpgsql;
