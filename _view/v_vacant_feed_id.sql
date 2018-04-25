CREATE VIEW cr_temp.v_vacant_feed_id as
WITH cte_sf (filled_id) AS (
    SELECT sf1.feed_id
    FROM audit.source_feeds sf1
    WHERE sf1.feed_id IS NOT NULL
    UNION SELECT sf2.source_id
          FROM audit.source_feeds sf2
          WHERE sf2.source_id IS NOT NULL

), cte_seq AS (
    SELECT row_number()
           OVER () AS num_seq
    FROM prod.action_header /*any huge table will do, cost is <0.45; generate_series() is leader-node only*/
    LIMIT 2999
), cte_vacant_id AS (

    SELECT
          coalesce(cte_sf.filled_id, num_seq) AS                  vacant_id
        , CASE WHEN num_seq BETWEEN 1 AND 999
        THEN '1. Inbound'
          WHEN num_seq BETWEEN 1000 AND 1999
              THEN '2. Outbound'
          WHEN num_seq BETWEEN 2000 AND 2999
              THEN '3. RPI/passthrough/internal cdh projects' END range_type


    FROM cte_seq
        FULL JOIN cte_sf
            ON num_seq = cte_sf.filled_id
    WHERE cte_sf.filled_id IS NULL
    ORDER BY 1
), cte_vacid_rank AS (

    SELECT
        vid.vacant_id
        , vid.range_type
        , row_number()
          OVER (
              PARTITION BY vid.range_type
              ORDER BY vid.range_type ASC, vid.vacant_id ASC ) vid_rank
    FROM cte_vacant_id vid
    where range_type is not null)

SELECT
    cte_vacid_rank.vacant_id
    , cte_vacid_rank.range_type
--     , cte_vacid_rank.vid_rank
FROM cte_vacid_rank
WHERE vid_rank <= 5
order by 2 asc

;