SELECT *
FROM PATTERN[
    (
        EVERY A=TestEvent(event_name='A') -> ((B=TestEvent(event_name='B')) where timer:within(3 sec))
    ) OR (
        EVERY B=TestEvent(event_name='B') -> ((A=TestEvent(event_name='A')) where timer:within(3 sec))
    )
]