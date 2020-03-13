SELECT * FROM TestEvent(event_name = ''A'').win:ext_timed(occur_time, 3 sec) AS A
GROUP BY A.dst_address
HAVING count(*) > {0}