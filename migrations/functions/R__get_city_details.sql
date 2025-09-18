drop function if exists get_city_details();
create or replace function get_city_details()
  RETURNS table
          (
            city text,
            call_count bigint,
            call_percent numeric,
            avg_response_time text,
            over_seven_resp_count bigint,
            over_seven_resp_percent numeric
          )
as
$$
begin
  return query
    SELECT sd.city,
           COUNT(1) AS "# Calls",
           ROUND(100.0 * COUNT(1) / SUM(COUNT(1)) OVER (), 1) AS "Call %%",
           TO_CHAR(
             MAKE_INTERVAL(secs => ROUND(AVG(response_time_seconds))),
             'MI:SS'
           ) AS "Avg Resp Time",
           COUNT(1) FILTER (WHERE response_time_seconds >= 420) AS "+7 Min Resp",
           round(((count(1) filter ( where response_time_seconds >= 420 ))::numeric / count(1)) * 100, 1) as "+7 Min Resp %%"
    FROM sheet_data sd
    GROUP BY sd.city
    ORDER BY count(1) desc;
end;
$$ language plpgsql;