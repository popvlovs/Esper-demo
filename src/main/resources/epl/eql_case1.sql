SELECT * FROM TestEvent
MATCH_RECOGNIZE (
    PARTITION BY src_address
    MEASURES
        A.src_address as A_SRC_ADDRESS,
        A.event_name  as A_EVENT_NAME,
        B.event_name  as B_EVENT_NAME,
        B.src_address as B_SRC_ADDRESS
    PATTERN (A B)
    DEFINE
        A as A.event_name = 'A1',
        B as B.event_name = 'B1' and B.src_address = A.src_address
)