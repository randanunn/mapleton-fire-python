drop function if exists get_overlap_details();
create or replace function get_overlap_details()
  RETURNS table
          (
            call_count bigint,
            call_percent numeric
          )
as
$$
begin
  return query
    select count(1) filter ( where overlap_previous is true ),
           round(((count(1) filter ( where overlap_previous is true ))::numeric / count(1) * 100), 1)
    from sheet_data;
end;
$$ LANGUAGE plpgsql SECURITY DEFINER;