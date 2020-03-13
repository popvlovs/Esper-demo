SELECT irstream * FROM TestEventPATTERN[every (
  A=TestEvent(event_name='A' AND src_address='172.16.100.1')
   OR
  B=TestEvent(event_name='B' AND dst_address='172.16.100.2')
)]#ext_timed(A.occur_time, 2 sec)