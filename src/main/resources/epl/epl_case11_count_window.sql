SELECT count(*) as win_count, dst_address FROM TestEvent(event_name = ''A'').win:ext_timed(occur_time, 3 sec) AS A
GROUP BY A.dst_address