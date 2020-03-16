SELECT irstream *
FROM TestEvent(
  ((event_name = 'A' AND src_address = '172.16.100.1') AND oncase('rule# ', 0, 10, `group`, *))
  or
  ((event_name = 'B' AND src_address = '172.16.100.2') AND oncase('rule#001', 1, 10, `group`, *))
  or
  ((event_name = 'C' AND src_address = '172.16.100.3') AND oncase('rule#001', 2, 10, `group`, *))
  or
  ((event_name = 'D' AND src_address = '172.16.100.4') AND oncase('rule#001', 3, 10, `group`, *))
  or
  ((event_name = 'E' AND src_address = '172.16.100.5') AND oncase('rule#001', 4, 10, `group`, *))
  or
  ((event_name = 'F' AND src_address = '172.16.100.6') AND oncase('rule#001', 5, 10, `group`, *))
  or
  ((event_name = 'G' AND src_address = '172.16.100.7') AND oncase('rule#001', 6, 10, `group`, *))
  or
  ((event_name = 'H' AND src_address = '172.16.100.8') AND oncase('rule#001', 7, 10, `group`, *))
  or
  ((event_name = 'I' AND src_address = '172.16.100.9') AND oncase('rule#001', 8, 10, `group`, *))
  or
  ((event_name = 'J' AND src_address = '172.16.100.10') AND oncase('rule#001', 9, 10, `group`, *))
).win:ext_timed(occur_time, 2 sec)
