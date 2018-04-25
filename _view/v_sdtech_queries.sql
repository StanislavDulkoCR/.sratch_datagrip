drop view if exists cr_temp.v_sdtech_queries;
create view cr_temp.v_sdtech_queries as
with tt1 as (/*v_sdtech_skip_override*/
    SELECT
          'select pg_terminate_backend(' || s.pid || '); '                                                                                                                                                                                  AS pid_terminate
        , trim(u.usename)                                                                                                                                                                                                                   AS username
        , s.pid
        , q.xid
        , q.query
        , q.service_class                                                                                                                                                                                                                   AS service_class
        , q.slot_count                                                                                                                                                                                                                      AS slt
        , date_trunc('second', q.wlm_start_time)                                                                                                                                                                                            AS start
        , decode(trim(q.state), 'Running', 'Run', 'QueuedWaiting', 'Queue', 'Returning', 'Return', trim(q.state))                                                                                                                           AS state
        , q.queue_time / 1000000                                                                                                                                                                                                            AS q_sec
        , q.exec_time / 1000000                                                                                                                                                                                                             AS exe_sec
        , m.cpu_time / 1000000                                                                                                                                                                                                                 cpu_sec
        , m.blocks_read                                                                                                                                                                                                                        read_mb
        , decode(m.blocks_to_disk, -1, NULL, m.blocks_to_disk)                                                                                                                                                                                 spill_mb
        , m2.rows                                                                                                                                                                                                                           AS ret_rows
        , m3.rows                                                                                                                                                                                                                           AS nl_rows
        , substring(replace(nvl(qrytext_cur.text, trim(translate(s.text, chr(10) || chr(13) || chr(9), ''))), '\\n', ' '), 1, 90)                                                                                                           AS sql
        , trim(decode(event & 1, 1, 'SK ', '') || decode(event & 2, 2, 'Del ', '') || decode(event & 4, 4, 'NL ', '') || decode(event & 8, 8, 'Dist ', '') || decode(event & 16, 16, 'Bcast ', '') || decode(event & 32, 32, 'Stats ', '')) AS alert

    FROM stv_wlm_query_state q LEFT OUTER JOIN stl_querytext s
            ON (s.query = q.query AND sequence = 0)
        LEFT OUTER JOIN stv_query_metrics m
            ON (q.query = m.query AND m.segment = -1 AND m.step = -1)
        LEFT OUTER JOIN stv_query_metrics m2
            ON (q.query = m2.query AND m2.step_type = 38)
        LEFT OUTER JOIN (SELECT
                             query
                             , sum(rows) AS rows
                         FROM stv_query_metrics m3
                         WHERE step_type = 15
                         GROUP BY 1) AS m3
            ON (q.query = m3.query)
        LEFT OUTER JOIN pg_user u
            ON (s.userid = u.usesysid)
        LEFT OUTER JOIN (SELECT
                             ut.xid
                             , 'CURSOR ' || TRIM(substring(text FROM strpos(upper(text), 'SELECT'))) AS text
                         FROM stl_utilitytext ut
                         WHERE sequence = 0 AND upper(text) LIKE 'DECLARE%'
                         GROUP BY text, ut.xid) qrytext_cur
            ON (q.xid = qrytext_cur.xid)
        LEFT OUTER JOIN (SELECT
                             query
                             , sum(decode(trim(split_part(event, ':', 1)), 'Very selective query filter', 1, 'Scanned a large number of deleted rows', 2, 'Nested Loop Join in the query plan', 4, 'Distributed a large number of rows across the network', 8, 'Broadcasted a large number of rows across the network', 16, 'Missing query planner statistics', 32, 0)) AS event
                         FROM stl_alert_event_log
                         WHERE event_time >= dateadd(HOUR, -8, current_Date)
                         GROUP BY query) AS alrt
            ON alrt.query = q.query
    ORDER BY q.service_class, q.exec_time DESC, q.wlm_start_time
)


SELECT
    tt1.pid_terminate
,	tt1.username
,	tt1.state
,	tt1.start
,	tt1.alert
,	tt1.sql
,	tt1.pid
,	tt1.xid
,	tt1.query
,	tt1.service_class
,	tt1.slt
,	tt1.q_sec
,	tt1.exe_sec
,	tt1.cpu_sec
,	tt1.read_mb
,	tt1.spill_mb
,	tt1.ret_rows
,	tt1.nl_rows

from tt1
where sql not like '%v_sdtech_skip_override%'
;