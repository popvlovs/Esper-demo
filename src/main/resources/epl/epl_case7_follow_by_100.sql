SELECT * FROM PATTERN [
    EVERY A=TestEvent(event_name=''A'' AND dst_address=''172.16.100.{0}'') -> ((B=TestEvent(event_name=''B'' AND dst_address=''172.16.100.{1}'')) where timer:within(3 sec))
]