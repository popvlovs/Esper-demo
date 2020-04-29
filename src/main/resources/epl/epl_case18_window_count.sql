SELECT
  count(*) as total,
  min(occur_time) as start_time,
  max(occur_time) as end_time,
  A.src_address as src_address
FROM TestEvent.win:ext_timed(occur_time, 1 min) as A
group by src_address
having count(*) > 100