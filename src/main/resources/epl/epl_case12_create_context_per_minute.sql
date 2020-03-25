create context CtxEachMinute
initiated by pattern [every timer:interval(1 min)]
terminated after 1 minutes

context CtxEachMinute
SELECT
    count(*) as win_count,
    A.event_name as event_name
FROM
    TestEvent.win:ext_timed(occur_time, 3 sec) AS A
GROUP BY
    A.event_name
OUTPUT SNAPSHOT WHEN TERMINATED