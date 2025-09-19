drop function if exists get_springville_details();
create or replace function get_springville_details()
  RETURNS table
          (
            call_count bigint,
            cancel_count bigint,
            cancel_percent numeric,
            avg_cancel_response text
          )
as
$$
begin
  return query
    select count(1),
           count(1) filter ( where call_enroute_time is not null and call_complete_time is not null and call_arrived_time is null ),
           round(((count(1) filter ( where call_enroute_time is not null and call_complete_time is not null and call_arrived_time is null ))::numeric / count(1)) * 100, 2),
           TO_CHAR(
             MAKE_INTERVAL(secs => round(avg(extract(epoch from call_complete_time - call_dispatched_time)) filter (
               where call_enroute_time is not null
                 and call_complete_time is not null
                 and call_arrived_time is null
               ))),
             'MI:SS'
           )

    from sheet_data
    where city = 'SPRINGVILLE';
end;
$$ LANGUAGE plpgsql SECURITY DEFINER;