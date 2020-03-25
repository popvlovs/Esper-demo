SELECT irstream *
FROM TestEvent(
  ((event_name = 'A' AND src_address = '172.16.100.1') AND oncase(1, 0, 10, *, `group`))
  or
  ((event_name = 'B' AND src_address = '172.16.100.2') AND oncase(1, 1, 10, *, `group`))
  or
  ((event_name = 'C' AND src_address = '172.16.100.3') AND oncase(1, 2, 10, *, `group`))
  or
  ((event_name = 'D' AND src_address = '172.16.100.4') AND oncase(1, 3, 10, *, `group`))
  or
  ((event_name = 'E' AND src_address = '172.16.100.5') AND oncase(1, 4, 10, *, `group`))
  or
  ((event_name = 'F' AND src_address = '172.16.100.6') AND oncase(1, 5, 10, *, `group`))
  or
  ((event_name = 'G' AND src_address = '172.16.100.7') AND oncase(1, 6, 10, *, `group`))
  or
  ((event_name = 'H' AND src_address = '172.16.100.8') AND oncase(1, 7, 10, *, `group`))
  or
  ((event_name = 'I' AND src_address = '172.16.100.9') AND oncase(1, 8, 10, *, `group`))
  or
  ((event_name = 'J' AND src_address = '172.16.100.10') AND oncase(1, 9, 10, *, `group`))
).win:ext_timed(occur_time, 2 sec)
