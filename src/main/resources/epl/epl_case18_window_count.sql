SELECT
  count(distinct(dst_address)) as total,
  window(dst_address) as dst_address,
  min(occur_time) as start_time,
  max(occur_time) as end_time,
  A.event_name as event_name,
  A.src_address as src_address
FROM TestEvent.win:ext_timed(occur_time, 1 min) as A
group by A.src_address
having count(distinct(dst_address)) > 100