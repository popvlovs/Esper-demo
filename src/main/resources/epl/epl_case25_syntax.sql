select
  a[0].occur_time as occur_time,
  a[0].event_name as event_name
from pattern [
  every[4] (a=TestEvent(event_name = 'A' and occur_time >= 95) where timer:within(3 min))
]