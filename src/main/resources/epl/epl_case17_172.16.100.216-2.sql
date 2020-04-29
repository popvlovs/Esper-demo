SELECT min(A.`occur_time`) AS `start_time`, max(A.`occur_time`) AS `end_time`, A.`src_address` AS `src_address`,
       A.`src_port` AS `src_port`, A.`dst_address` AS `dst_address`, A.`dst_port` AS `dst_port`,
       WINDOW(id) AS _WINDOW_ID, WINDOW(event_id) AS _WINDOW_IDS, WINDOW(src_address) AS _WINDOW_ENRICH_SIP,
       WINDOW(src_address_array) AS _WINDOW_ENRICH_SIPS, WINDOW(dst_address) AS _WINDOW_ENRICH_DIP,
       WINDOW(dst_address_array) AS _WINDOW_ENRICH_DIPS, WINDOW(data_source) AS _WINDOW_ENRICH_DATASOURCE,
       WINDOW(data_source_array) AS _WINDOW_ENRICH_DATASOURCES
FROM GlobalEvent(spin_tag = 0L AND (belongs(`src_address`, 'CSWLHT4101a6') and belongs(`dst_address`, 'CY2KATIS74d0')
and belongs(`dst_address`, 'CSWLHT4101a6') and (`dst_port` = 53 or contains(`app_protocol`, "dns")))).win:ext_timed(occur_time, 1 min) AS A
GROUP BY A.`alarm_level`
HAVING count(*)>= 100