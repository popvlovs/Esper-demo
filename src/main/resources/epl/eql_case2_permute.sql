SELECT * FROM TestEvent
MATCH_RECOGNIZE (
    PARTITION BY src_address
    MEASURES
        A.src_address as A_SRC_ADDRESS,
        A.event_name  as A_EVENT_NAME,
        B.src_address as B_SRC_ADDRESS,
        B.event_name  as B_EVENT_NAME
    PATTERN (MATCH_RECOGNIZE_PERMUTE(A, B, C, D, E))
    DEFINE
        A as A.event_name = ''A'' and A.dst_address = ''172.16.100.1'',
        B as B.event_name = ''B'' and B.dst_address = ''172.16.100.1'',
        C as C.event_name = ''C'' and C.dst_address = ''172.16.100.1'',
        D as D.event_name = ''D'' and D.dst_address = ''172.16.100.1'',
        E as E.event_name = ''E'' and E.dst_address = ''172.16.100.1''
)