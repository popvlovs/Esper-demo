SELECT irstream *
FROM GlobalEvent(
  ((event_name = 'A' ) AND oncase(1, 0, 10, *, `group`))
  or
  ((event_name = 'B' ) AND oncase(1, 1, 10, *, `group`))
  or
  ((event_name = 'C' ) AND oncase(1, 2, 10, *, `group`))
  or
  ((event_name = 'D' ) AND oncase(1, 3, 10, *, `group`))
  or
  ((event_name = 'E' ) AND oncase(1, 4, 10, *, `group`))
  or
  ((event_name = 'F' ) AND oncase(1, 5, 10, *, `group`))
  or
  ((event_name = 'G' ) AND oncase(1, 6, 10, *, `group`))
  or
  ((event_name = 'H' ) AND oncase(1, 7, 10, *, `group`))
  or
  ((event_name = 'I' ) AND oncase(1, 8, 10, *, `group`))
  or
  ((event_name = 'J' ) AND oncase(1, 9, 10, *, `group`))
).win:ext_timed(occur_time, 2 sec)
HAVING (TRUE)