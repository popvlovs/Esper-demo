SELECT
count(*) as win_count,
min(A.event_id) as start_id,
max(A.event_id) as end_id,
min(A.occur_time) as start_time,
max(A.occur_time) as end_time,
A.event_name as event_name,
A.group_0
FROM
TestEvent(event_name = 'A').win:ext_timed_batch(occur_time, 1 sec) AS A
GROUP BY
A.group_0