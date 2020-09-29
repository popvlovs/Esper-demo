SELECT
A.event_name as event_name,
A.src_address as src_address
FROM
PATTERN[
    EVERY A=TestEvent(event_name='A')
            -> (B=TestEvent(event_name='C') WHERE timer:within(1 min))
    WHILE(A.occur_time <=B.occur_time AND B.occur_time - A.occur_time <=60000)
]