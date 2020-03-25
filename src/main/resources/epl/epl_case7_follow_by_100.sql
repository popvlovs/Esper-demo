SELECT
    A.`occur_time` AS `start_time`,
    A.`event_id` AS `process_id`,
    B.`dst_address` AS `dst_address`,
    B.`occur_time` AS `end_time`, *
FROM PATTERN [
    EVERY A=TestEvent(event_name=''A'' AND dst_address=''172.16.100.{0}'') -> ((B=TestEvent(event_name=''B'' AND dst_address=''172.16.100.{1}'')) where timer:within(3 sec))
]