drop function if exists get_mapleton_details();
create or replace function get_mapleton_details()
  RETURNS table
          (
            timeframe text,
            call_count bigint,
            call_percent numeric
          )
as
$$
begin
  return query
    select "Timeframe", "Call Count", "Call %%"
    from (with t1 as (select *
                      from sheet_data
                      where city = 'MAPLETON')
          select 'Canceled Prior to Arrival'                                                                          as "Timeframe",
                 count(1) filter ( where response_time_seconds is null)                                       as "Call Count",
                 round((count(1) filter ( where response_time_seconds is null )::numeric / count(1)) * 100,
                       1)                                                                                    as "Call %%",
                 1                                                                                           as sort_order
          from t1
          UNION
          select 'Less than 5 mins'                                                                          as "Timeframe",
                 count(1) filter ( where response_time_seconds < 300 )                                       as "Call Count",
                 round((count(1) filter ( where response_time_seconds < 300 )::numeric / count(1)) * 100,
                       1)                                                                                    as "Call %%",
                 2                                                                                          as sort_order
          from t1
          UNION
          select '5 - 7 Minutes'                                                                          as "Timeframe",
                 count(1)
                 filter ( where response_time_seconds >= 300 and response_time_seconds < 420 )          as "Call Count",
                 round((count(1)
                        filter ( where response_time_seconds >= 300 and response_time_seconds < 420 )::numeric /
                        count(1)) * 100,
                       1)                                                                               as "Call %%",
                 3                                                                                      as sort_order
          from t1
          UNION
          select '7 - 9 Minutes'                                                                          as "Timeframe",
                 count(1)
                 filter ( where response_time_seconds >= 420 and response_time_seconds < 540 )          as "Call Count",
                 round((count(1)
                        filter ( where response_time_seconds >= 420 and response_time_seconds < 540 )::numeric /
                        count(1)) * 100,
                       1)                                                                               as "Call %%",
                 4                                                                                      as sort_order
          from t1
          union
          select '+ 9 Minutes'                                                                                 as "Timeframe",
                 count(1) filter ( where response_time_seconds >= 540 )                                       as "Call Count",
                 round((count(1) filter ( where response_time_seconds >= 540 )::numeric / count(1)) * 100,
                       1)                                                                                     as "Call %%",
                 5                                                                                            as sort_order
          from t1) as t2 order by sort_order;
end;
$$ LANGUAGE plpgsql SECURITY DEFINER;