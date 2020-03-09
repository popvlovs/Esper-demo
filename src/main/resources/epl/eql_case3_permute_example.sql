select * from TestEvent
match_recognize (
  partition by device
  measures A.id as a_id, B.id as b_id
  pattern (match_recognize_permute(A, B))
  define
    A as A.temp < 100,
    B as B.temp >= 100)