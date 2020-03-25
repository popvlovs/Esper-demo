SELECT
    count(*) as win_count,
    min(A.event_id) as start_id,
    max(A.event_id) as end_id,
    min(A.occur_time) as start_time,
    max(A.occur_time) as end_time,
    A.event_name as event_name,
    A.group_0, A.group_1, A.group_2,A.group_3, A.group_4, A.group_5,A.group_6, A.group_7, A.group_8,A.group_9
FROM
    TestEvent.win:ext_timed_batch(occur_time, 1 sec) AS A
GROUP BY
    A.event_name, A.group_0, A.group_1, A.group_2,A.group_3, A.group_4, A.group_5,A.group_6, A.group_7, A.group_8,A.group_9