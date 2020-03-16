SELECT irstream *
FROM TestEvent(
  ((event_name = 'A' ) AND oncase('rule#001', 0, 10, `group`, *))
  or
  ((event_name = 'B' ) AND oncase('rule#001', 1, 10, `group`, *))
  or
  ((event_name = 'C' ) AND oncase('rule#001', 2, 10, `group`, *))
  or
  ((event_name = 'D' ) AND oncase('rule#001', 3, 10, `group`, *))
  or
  ((event_name = 'E' ) AND oncase('rule#001', 4, 10, `group`, *))
  or
  ((event_name = 'F' ) AND oncase('rule#001', 5, 10, `group`, *))
  or
  ((event_name = 'G' ) AND oncase('rule#001', 6, 10, `group`, *))
  or
  ((event_name = 'H' ) AND oncase('rule#001', 7, 10, `group`, *))
  or
  ((event_name = 'I' ) AND oncase('rule#001', 8, 10, `group`, *))
  or
  ((event_name = 'J' ) AND oncase('rule#001', 9, 10, `group`, *))
).win:ext_timed(occur_time, 2 sec)
