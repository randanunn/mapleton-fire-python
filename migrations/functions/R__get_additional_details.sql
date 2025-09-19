drop function if exists get_additional_details();
create or replace function get_additional_details()
  RETURNS table
          (
            data_point text,
            all_calls text,
            mapleton text
          )
as
$$
begin
  return query
    select 'Call Creation Until Dispatched' as "Data Point",
           TO_CHAR(
             MAKE_INTERVAL(secs => round(avg(extract(epoch from call_dispatched_time - call_psap_time)) filter (
               where call_psap_time is not null and call_dispatched_time is not null
               ))),
             'MI:SS'
           ) AS "All Calls",
           TO_CHAR(
             MAKE_INTERVAL(secs => round(avg(extract(epoch from call_dispatched_time - call_psap_time)) filter (
               where call_psap_time is not null and call_dispatched_time is not null and city = 'MAPLETON'
               ))),
             'MI:SS'
           ) AS "Mapleton Only"
    from sheet_data
    union
    select 'Turnout Time' as "Data Point",
           TO_CHAR(
             MAKE_INTERVAL(secs => round(avg(extract(epoch from call_enroute_time - call_dispatched_time)) filter (
               where call_enroute_time is not null and call_dispatched_time is not null
               ))),
             'MI:SS'
           ) AS "All Calls",
           TO_CHAR(
             MAKE_INTERVAL(secs => round(avg(extract(epoch from call_enroute_time - call_dispatched_time)) filter (
               where call_enroute_time is not null and call_dispatched_time is not null and city = 'MAPLETON'
               ))),
             'MI:SS'
           ) AS "Mapleton Only"
    from sheet_data;
end;
$$ LANGUAGE plpgsql SECURITY DEFINER;