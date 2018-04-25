
drop view cr_temp.v_sdtech_gatekeeper;
;
-- truncate table cr_temp.automation_gatekeeper_dependency;
;
;


CREATE OR REPLACE VIEW cr_temp.v_sdtech_gatekeeper
AS
WITH cte_level AS (
SELECT
    agd.primary_id
    , agd.secondary_id
    , agd.gate_status
    , agd.gate_opened_dt
    , agd.gate_closed_dt
    , agd.automation_name
    , agd.enabled
    , coalesce(fc1.feed_name, sf1.feed_name) as primary_name
    , coalesce(fc2.feed_name, sf2.feed_name) as secondary_name
FROM audit.automation_gatekeeper agd
    LEFT JOIN audit.feed_config fc1
        ON agd.primary_id = fc1.feed_id
    LEFT JOIN audit.feed_config fc2
        ON agd.secondary_id = fc2.feed_id
    left join audit.source_feeds sf1
        on agd.primary_id = sf1.feed_id
    left join audit.source_feeds sf2
        on agd.secondary_id = sf2.feed_id

)

, cte_full_dataset as (
    SELECT
        cast('\t' as varchar(1)) /*cast(repeat(' ',4) as varchar(4))*/ as _tech_indent
        , l1.primary_id      AS l1_primary_id          , l1.primary_name    AS l1_primary_name         , l1.secondary_id    AS l1_secondary_id         , l1.secondary_name  AS l1_secondary_name
        , l2.primary_id      AS l2_primary_id          , l2.primary_name    AS l2_primary_name         , l2.secondary_id    AS l2_secondary_id         , l2.secondary_name  AS l2_secondary_name
        , l3.primary_id      AS l3_primary_id          , l3.primary_name    AS l3_primary_name         , l3.secondary_id    AS l3_secondary_id         , l3.secondary_name  AS l3_secondary_name
        , l4.primary_id      AS l4_primary_id          , l4.primary_name    AS l4_primary_name         , l4.secondary_id    AS l4_secondary_id         , l4.secondary_name  AS l4_secondary_name
        , l5.primary_id      AS l5_primary_id          , l5.primary_name    AS l5_primary_name         , l5.secondary_id    AS l5_secondary_id         , l5.secondary_name  AS l5_secondary_name
        , l6.primary_id      AS l6_primary_id          , l6.primary_name    AS l6_primary_name         , l6.secondary_id    AS l6_secondary_id         , l6.secondary_name  AS l6_secondary_name
        , l7.primary_id      AS l7_primary_id          , l7.primary_name    AS l7_primary_name         , l7.secondary_id    AS l7_secondary_id         , l7.secondary_name  AS l7_secondary_name
        , l8.primary_id      AS l8_primary_id          , l8.primary_name    AS l8_primary_name         , l8.secondary_id    AS l8_secondary_id         , l8.secondary_name  AS l8_secondary_name
        , l9.primary_id      AS l9_primary_id          , l9.primary_name    AS l9_primary_name         , l9.secondary_id    AS l9_secondary_id         , l9.secondary_name  AS l9_secondary_name
        , l10.primary_id     AS l10_primary_id         , l10.primary_name   AS l10_primary_name        , l10.secondary_id   AS l10_secondary_id        , l10.secondary_name AS l10_secondary_name
        , l11.primary_id     AS l11_primary_id         , l11.primary_name   AS l11_primary_name        , l11.secondary_id   AS l11_secondary_id        , l11.secondary_name AS l11_secondary_name
        , l12.primary_id     AS l12_primary_id         , l12.primary_name   AS l12_primary_name        , l12.secondary_id   AS l12_secondary_id        , l12.secondary_name AS l12_secondary_name
        , l13.primary_id     AS l13_primary_id         , l13.primary_name   AS l13_primary_name        , l13.secondary_id   AS l13_secondary_id        , l13.secondary_name AS l13_secondary_name
        , l14.primary_id     AS l14_primary_id         , l14.primary_name   AS l14_primary_name        , l14.secondary_id   AS l14_secondary_id        , l14.secondary_name AS l14_secondary_name
        , l15.primary_id     AS l15_primary_id         , l15.primary_name   AS l15_primary_name        , l15.secondary_id   AS l15_secondary_id        , l15.secondary_name AS l15_secondary_name
        , l16.primary_id     AS l16_primary_id         , l16.primary_name   AS l16_primary_name        , l16.secondary_id   AS l16_secondary_id        , l16.secondary_name AS l16_secondary_name
        , l17.primary_id     AS l17_primary_id         , l17.primary_name   AS l17_primary_name        , l17.secondary_id   AS l17_secondary_id        , l17.secondary_name AS l17_secondary_name
        , l18.primary_id     AS l18_primary_id         , l18.primary_name   AS l18_primary_name        , l18.secondary_id   AS l18_secondary_id        , l18.secondary_name AS l18_secondary_name
        , l19.primary_id     AS l19_primary_id         , l19.primary_name   AS l19_primary_name        , l19.secondary_id   AS l19_secondary_id        , l19.secondary_name AS l19_secondary_name
        , l20.primary_id     AS l20_primary_id         , l20.primary_name   AS l20_primary_name        , l20.secondary_id   AS l20_secondary_id        , l20.secondary_name AS l20_secondary_name

        , case when l1.primary_id = 10101 then 1 else null end       AS l1_depth
        , decode(l2.primary_id ,null,null,2)           AS l2_depth
        , decode(l3.primary_id ,null,null,3)           AS l3_depth
        , decode(l4.primary_id ,null,null,4)           AS l4_depth
        , decode(l5.primary_id ,null,null,5)           AS l5_depth
        , decode(l6.primary_id ,null,null,6)           AS l6_depth
        , decode(l7.primary_id ,null,null,7)           AS l7_depth
        , decode(l8.primary_id ,null,null,8)           AS l8_depth
        , decode(l9.primary_id ,null,null,9)           AS l9_depth
        , decode(l10.primary_id,null,null,10)          AS l10_depth
        , decode(l11.primary_id,null,null,11)          AS l11_depth
        , decode(l12.primary_id,null,null,12)          AS l12_depth
        , decode(l13.primary_id,null,null,13)          AS l13_depth
        , decode(l14.primary_id,null,null,14)          AS l14_depth
        , decode(l15.primary_id,null,null,15)          AS l15_depth
        , decode(l16.primary_id,null,null,16)          AS l16_depth
        , decode(l17.primary_id,null,null,17)          AS l17_depth
        , decode(l18.primary_id,null,null,18)          AS l18_depth
        , decode(l19.primary_id,null,null,19)          AS l19_depth
        , decode(l20.primary_id,null,null,20)          AS l20_depth

        , l1.enabled  as l1_enabled
        , l2.enabled  as l2_enabled
        , l3.enabled  as l3_enabled
        , l4.enabled  as l4_enabled
        , l5.enabled  as l5_enabled
        , l6.enabled  as l6_enabled
        , l7.enabled  as l7_enabled
        , l8.enabled  as l8_enabled
        , l9.enabled  as l9_enabled
        , l10.enabled as l10_enabled
        , l11.enabled as l11_enabled
        , l12.enabled as l12_enabled
        , l13.enabled as l13_enabled
        , l14.enabled as l14_enabled
        , l15.enabled as l15_enabled
        , l16.enabled as l16_enabled
        , l17.enabled as l17_enabled
        , l18.enabled as l18_enabled
        , l19.enabled as l19_enabled
        , l20.enabled as l20_enabled

        , l1.gate_status  as l1_gate_status
        , l2.gate_status  as l2_gate_status
        , l3.gate_status  as l3_gate_status
        , l4.gate_status  as l4_gate_status
        , l5.gate_status  as l5_gate_status
        , l6.gate_status  as l6_gate_status
        , l7.gate_status  as l7_gate_status
        , l8.gate_status  as l8_gate_status
        , l9.gate_status  as l9_gate_status
        , l10.gate_status as l10_gate_status
        , l11.gate_status as l11_gate_status
        , l12.gate_status as l12_gate_status
        , l13.gate_status as l13_gate_status
        , l14.gate_status as l14_gate_status
        , l15.gate_status as l15_gate_status
        , l16.gate_status as l16_gate_status
        , l17.gate_status as l17_gate_status
        , l18.gate_status as l18_gate_status
        , l19.gate_status as l19_gate_status
        , l20.gate_status as l20_gate_status
        /*

        , dense_rank() over (order by l1.depth, l1.primary_id) as rank_order*/

    FROM cte_level l1
        LEFT JOIN cte_level l2  ON l1.secondary_id  = l2.primary_id    /*and l1.primary_id != 10101*/
        LEFT JOIN cte_level l3  ON l2.secondary_id  = l3.primary_id    /*and l1.primary_id != 10101*/
        LEFT JOIN cte_level l4  ON l3.secondary_id  = l4.primary_id    /*and l1.primary_id != 10101*/
        LEFT JOIN cte_level l5  ON l4.secondary_id  = l5.primary_id    /*and l1.primary_id != 10101*/
        LEFT JOIN cte_level l6  ON l5.secondary_id  = l6.primary_id    /*and l1.primary_id != 10101*/
        LEFT JOIN cte_level l7  ON l6.secondary_id  = l7.primary_id    /*and l1.primary_id != 10101*/
        LEFT JOIN cte_level l8  ON l7.secondary_id  = l8.primary_id    /*and l1.primary_id != 10101*/
        LEFT JOIN cte_level l9  ON l8.secondary_id  = l9.primary_id    /*and l1.primary_id != 10101*/
        LEFT JOIN cte_level l10 ON l9.secondary_id  = l10.primary_id   /*and l1.primary_id != 10101*/
        LEFT JOIN cte_level l11 ON l10.secondary_id = l11.primary_id   /*and l1.primary_id != 10101*/
        LEFT JOIN cte_level l12 ON l11.secondary_id = l12.primary_id   /*and l1.primary_id != 10101*/
        LEFT JOIN cte_level l13 ON l12.secondary_id = l13.primary_id   /*and l1.primary_id != 10101*/
        LEFT JOIN cte_level l14 ON l13.secondary_id = l14.primary_id   /*and l1.primary_id != 10101*/
        LEFT JOIN cte_level l15 ON l14.secondary_id = l15.primary_id   /*and l1.primary_id != 10101*/
        LEFT JOIN cte_level l16 ON l15.secondary_id = l16.primary_id   /*and l1.primary_id != 10101*/
        LEFT JOIN cte_level l17 ON l16.secondary_id = l17.primary_id   /*and l1.primary_id != 10101*/
        LEFT JOIN cte_level l18 ON l17.secondary_id = l18.primary_id   /*and l1.primary_id != 10101*/
        LEFT JOIN cte_level l19 ON l18.secondary_id = l19.primary_id   /*and l1.primary_id != 10101*/
        LEFT JOIN cte_level l20 ON l19.secondary_id = l20.primary_id   /*and l1.primary_id != 10101*/



)
--     select * from cte_full_dataset;
    , cte_ranking as( select
                        *
                        , decode(decode(l1_primary_id ,null,null,1)  ,null,0,1)
                        + decode(decode(l2_primary_id ,null,null,2)  ,null,0,1)
                        + decode(decode(l3_primary_id ,null,null,3)  ,null,0,1)
                        + decode(decode(l4_primary_id ,null,null,4)  ,null,0,1)
                        + decode(decode(l5_primary_id ,null,null,5)  ,null,0,1)
                        + decode(decode(l6_primary_id ,null,null,6)  ,null,0,1)
                        + decode(decode(l7_primary_id ,null,null,7)  ,null,0,1)
                        + decode(decode(l8_primary_id ,null,null,8)  ,null,0,1)
                        + decode(decode(l9_primary_id ,null,null,9)  ,null,0,1)
                        + decode(decode(l10_primary_id,null,null,10) ,null,0,1)
                        + decode(decode(l11_primary_id,null,null,11) ,null,0,1)
                        + decode(decode(l12_primary_id,null,null,12) ,null,0,1)
                        + decode(decode(l13_primary_id,null,null,13) ,null,0,1)
                        + decode(decode(l14_primary_id,null,null,14) ,null,0,1)
                        + decode(decode(l15_primary_id,null,null,15) ,null,0,1)
                        + decode(decode(l16_primary_id,null,null,16) ,null,0,1)
                        + decode(decode(l17_primary_id,null,null,17) ,null,0,1)
                        + decode(decode(l18_primary_id,null,null,18) ,null,0,1)
                        + decode(decode(l19_primary_id,null,null,19) ,null,0,1)
                        + decode(decode(l20_primary_id,null,null,20) ,null,0,1) AS depth_max

                        from cte_full_dataset
                            where l1_depth = 1
)

-- select * from cte_ranking;
,  cte_fin (rank_order, rank_order_tiebreaker ,id, depth, hierarchy, enabled, gate_status) as (



--     select distinct (l1_primary_id * 100000)+1  ,1 , l1_primary_id  , l1_depth  , concat(repeat(_tech_indent,l1_depth  -1),l1_primary_name ), l1_enabled  ,l1_gate_status from cte_ranking where l1_primary_id is not null
--     union ALL
--     select distinct (l1_primary_id * 100000)+1  ,1 , l2_primary_id  , l2_depth  , concat(repeat(_tech_indent,l2_depth  -1),l2_primary_name ), l2_enabled  ,l2_gate_status from cte_ranking where l2_primary_id is not null






/*spacer*/select distinct (l1_primary_id * 100000+l1_secondary_id*1000)+1  ,1 , l1_primary_id  , l1_depth  , concat(repeat(_tech_indent,l1_depth  -1),l1_primary_name ), l1_enabled  ,l1_gate_status  from cte_ranking where l1_primary_id  is not null union all select distinct (l1_primary_id * 100000+l1_secondary_id*1000)+2 , 0, l1_secondary_id   , l1_depth  +1  , concat(repeat(_tech_indent,l1_depth +0),l1_secondary_name ), l1_enabled  , l1_gate_status  from cte_ranking where l1_primary_id is not null  and depth_max = 1
union all select distinct (l1_primary_id * 100000+l1_secondary_id*1000)+2  ,1 , l2_primary_id  , l2_depth  , concat(repeat(_tech_indent,l2_depth  -1),l2_primary_name ), l2_enabled  ,l2_gate_status  from cte_ranking where l2_primary_id  is not null union all select distinct (l1_primary_id * 100000+l1_secondary_id*1000)+3 , 0, l2_secondary_id   , l2_depth  +1  , concat(repeat(_tech_indent,l2_depth +0),l2_secondary_name ), l2_enabled  , l2_gate_status  from cte_ranking where l2_primary_id is not null  and depth_max = 2
union all select distinct (l1_primary_id * 100000+l1_secondary_id*1000)+3  ,1 , l3_primary_id  , l3_depth  , concat(repeat(_tech_indent,l3_depth  -1),l3_primary_name ), l3_enabled  ,l3_gate_status  from cte_ranking where l3_primary_id  is not null union all select distinct (l1_primary_id * 100000+l1_secondary_id*1000)+4 , 0, l3_secondary_id   , l3_depth  +1  , concat(repeat(_tech_indent,l3_depth +0),l3_secondary_name ), l3_enabled  , l3_gate_status  from cte_ranking where l3_primary_id is not null  and depth_max = 3
union all select distinct (l1_primary_id * 100000+l1_secondary_id*1000)+4  ,1 , l4_primary_id  , l4_depth  , concat(repeat(_tech_indent,l4_depth  -1),l4_primary_name ), l4_enabled  ,l4_gate_status  from cte_ranking where l4_primary_id  is not null union all select distinct (l1_primary_id * 100000+l1_secondary_id*1000)+5 , 0, l4_secondary_id   , l4_depth  +1  , concat(repeat(_tech_indent,l4_depth +0),l4_secondary_name ), l4_enabled  , l4_gate_status  from cte_ranking where l4_primary_id is not null  and depth_max = 4
union all select distinct (l1_primary_id * 100000+l1_secondary_id*1000)+5  ,1 , l5_primary_id  , l5_depth  , concat(repeat(_tech_indent,l5_depth  -1),l5_primary_name ), l5_enabled  ,l5_gate_status  from cte_ranking where l5_primary_id  is not null union all select distinct (l1_primary_id * 100000+l1_secondary_id*1000)+6 , 0, l5_secondary_id   , l5_depth  +1  , concat(repeat(_tech_indent,l5_depth +0),l5_secondary_name ), l5_enabled  , l5_gate_status  from cte_ranking where l5_primary_id is not null  and depth_max = 5
union all select distinct (l1_primary_id * 100000+l1_secondary_id*1000)+6  ,1 , l6_primary_id  , l6_depth  , concat(repeat(_tech_indent,l6_depth  -1),l6_primary_name ), l6_enabled  ,l6_gate_status  from cte_ranking where l6_primary_id  is not null union all select distinct (l1_primary_id * 100000+l1_secondary_id*1000)+7 , 0, l6_secondary_id   , l6_depth  +1  , concat(repeat(_tech_indent,l6_depth +0),l6_secondary_name ), l6_enabled  , l6_gate_status  from cte_ranking where l6_primary_id is not null  and depth_max = 6
union all select distinct (l1_primary_id * 100000+l1_secondary_id*1000)+7  ,1 , l7_primary_id  , l7_depth  , concat(repeat(_tech_indent,l7_depth  -1),l7_primary_name ), l7_enabled  ,l7_gate_status  from cte_ranking where l7_primary_id  is not null union all select distinct (l1_primary_id * 100000+l1_secondary_id*1000)+8 , 0, l7_secondary_id   , l7_depth  +1  , concat(repeat(_tech_indent,l7_depth +0),l7_secondary_name ), l7_enabled  , l7_gate_status  from cte_ranking where l7_primary_id is not null  and depth_max = 7
union all select distinct (l1_primary_id * 100000+l1_secondary_id*1000)+8  ,1 , l8_primary_id  , l8_depth  , concat(repeat(_tech_indent,l8_depth  -1),l8_primary_name ), l8_enabled  ,l8_gate_status  from cte_ranking where l8_primary_id  is not null union all select distinct (l1_primary_id * 100000+l1_secondary_id*1000)+9 , 0, l8_secondary_id   , l8_depth  +1  , concat(repeat(_tech_indent,l8_depth +0),l8_secondary_name ), l8_enabled  , l8_gate_status  from cte_ranking where l8_primary_id is not null  and depth_max = 8
union all select distinct (l1_primary_id * 100000+l1_secondary_id*1000)+9  ,1 , l9_primary_id  , l9_depth  , concat(repeat(_tech_indent,l9_depth  -1),l9_primary_name ), l9_enabled  ,l9_gate_status  from cte_ranking where l9_primary_id  is not null union all select distinct (l1_primary_id * 100000+l1_secondary_id*1000)+10, 0, l9_secondary_id   , l9_depth  +1  , concat(repeat(_tech_indent,l9_depth +0),l9_secondary_name ), l9_enabled  , l9_gate_status  from cte_ranking where l9_primary_id is not null  and depth_max = 9
union all select distinct (l1_primary_id * 100000+l1_secondary_id*1000)+10 ,1 , l10_primary_id , l10_depth , concat(repeat(_tech_indent,l10_depth -1),l10_primary_name), l10_enabled ,l10_gate_status from cte_ranking where l10_primary_id is not null union all select distinct (l1_primary_id * 100000+l1_secondary_id*1000)+11, 0, l10_secondary_id  , l10_depth +1 , concat(repeat(_tech_indent,l10_depth +0),l10_secondary_name), l10_enabled , l10_gate_status from cte_ranking where l10_primary_id is not null and depth_max = 10
union all select distinct (l1_primary_id * 100000+l1_secondary_id*1000)+11 ,1 , l11_primary_id , l11_depth , concat(repeat(_tech_indent,l11_depth -1),l11_primary_name), l11_enabled ,l11_gate_status from cte_ranking where l11_primary_id is not null union all select distinct (l1_primary_id * 100000+l1_secondary_id*1000)+12, 0, l11_secondary_id  , l11_depth +1 , concat(repeat(_tech_indent,l11_depth +0),l11_secondary_name), l11_enabled , l11_gate_status from cte_ranking where l11_primary_id is not null and depth_max = 11
union all select distinct (l1_primary_id * 100000+l1_secondary_id*1000)+12 ,1 , l12_primary_id , l12_depth , concat(repeat(_tech_indent,l12_depth -1),l12_primary_name), l12_enabled ,l12_gate_status from cte_ranking where l12_primary_id is not null union all select distinct (l1_primary_id * 100000+l1_secondary_id*1000)+13, 0, l12_secondary_id  , l12_depth +1 , concat(repeat(_tech_indent,l12_depth +0),l12_secondary_name), l12_enabled , l12_gate_status from cte_ranking where l12_primary_id is not null and depth_max = 12
union all select distinct (l1_primary_id * 100000+l1_secondary_id*1000)+13 ,1 , l13_primary_id , l13_depth , concat(repeat(_tech_indent,l13_depth -1),l13_primary_name), l13_enabled ,l13_gate_status from cte_ranking where l13_primary_id is not null union all select distinct (l1_primary_id * 100000+l1_secondary_id*1000)+14, 0, l13_secondary_id  , l13_depth +1 , concat(repeat(_tech_indent,l13_depth +0),l13_secondary_name), l13_enabled , l13_gate_status from cte_ranking where l13_primary_id is not null and depth_max = 13
union all select distinct (l1_primary_id * 100000+l1_secondary_id*1000)+14 ,1 , l14_primary_id , l14_depth , concat(repeat(_tech_indent,l14_depth -1),l14_primary_name), l14_enabled ,l14_gate_status from cte_ranking where l14_primary_id is not null union all select distinct (l1_primary_id * 100000+l1_secondary_id*1000)+15, 0, l14_secondary_id  , l14_depth +1 , concat(repeat(_tech_indent,l14_depth +0),l14_secondary_name), l14_enabled , l14_gate_status from cte_ranking where l14_primary_id is not null and depth_max = 14
union all select distinct (l1_primary_id * 100000+l1_secondary_id*1000)+15 ,1 , l15_primary_id , l15_depth , concat(repeat(_tech_indent,l15_depth -1),l15_primary_name), l15_enabled ,l15_gate_status from cte_ranking where l15_primary_id is not null union all select distinct (l1_primary_id * 100000+l1_secondary_id*1000)+16, 0, l15_secondary_id  , l15_depth +1 , concat(repeat(_tech_indent,l15_depth +0),l15_secondary_name), l15_enabled , l15_gate_status from cte_ranking where l15_primary_id is not null and depth_max = 15
union all select distinct (l1_primary_id * 100000+l1_secondary_id*1000)+16 ,1 , l16_primary_id , l16_depth , concat(repeat(_tech_indent,l16_depth -1),l16_primary_name), l16_enabled ,l16_gate_status from cte_ranking where l16_primary_id is not null union all select distinct (l1_primary_id * 100000+l1_secondary_id*1000)+17, 0, l16_secondary_id  , l16_depth +1 , concat(repeat(_tech_indent,l16_depth +0),l16_secondary_name), l16_enabled , l16_gate_status from cte_ranking where l16_primary_id is not null and depth_max = 16
union all select distinct (l1_primary_id * 100000+l1_secondary_id*1000)+17 ,1 , l17_primary_id , l17_depth , concat(repeat(_tech_indent,l17_depth -1),l17_primary_name), l17_enabled ,l17_gate_status from cte_ranking where l17_primary_id is not null union all select distinct (l1_primary_id * 100000+l1_secondary_id*1000)+18, 0, l17_secondary_id  , l17_depth +1 , concat(repeat(_tech_indent,l17_depth +0),l17_secondary_name), l17_enabled , l17_gate_status from cte_ranking where l17_primary_id is not null and depth_max = 17
union all select distinct (l1_primary_id * 100000+l1_secondary_id*1000)+18 ,1 , l18_primary_id , l18_depth , concat(repeat(_tech_indent,l18_depth -1),l18_primary_name), l18_enabled ,l18_gate_status from cte_ranking where l18_primary_id is not null union all select distinct (l1_primary_id * 100000+l1_secondary_id*1000)+19, 0, l18_secondary_id  , l18_depth +1 , concat(repeat(_tech_indent,l18_depth +0),l18_secondary_name), l18_enabled , l18_gate_status from cte_ranking where l18_primary_id is not null and depth_max = 18
union all select distinct (l1_primary_id * 100000+l1_secondary_id*1000)+19 ,1 , l19_primary_id , l19_depth , concat(repeat(_tech_indent,l19_depth -1),l19_primary_name), l19_enabled ,l19_gate_status from cte_ranking where l19_primary_id is not null union all select distinct (l1_primary_id * 100000+l1_secondary_id*1000)+20, 0, l19_secondary_id  , l19_depth +1 , concat(repeat(_tech_indent,l19_depth +0),l19_secondary_name), l19_enabled , l19_gate_status from cte_ranking where l19_primary_id is not null and depth_max = 19
union all select distinct (l1_primary_id * 100000+l1_secondary_id*1000)+20 ,1 , l20_primary_id , l20_depth , concat(repeat(_tech_indent,l20_depth -1),l20_primary_name), l20_enabled ,l20_gate_status from cte_ranking where l20_primary_id is not null union all select distinct (l1_primary_id * 100000+l1_secondary_id*1000)+21, 0, l20_secondary_id  , l20_depth +1 , concat(repeat(_tech_indent,l20_depth +0),l20_secondary_name), l20_enabled , l20_gate_status from cte_ranking where l20_primary_id is not null and depth_max = 20

)

select
      id
    , depth
    , hierarchy
    , gate_status
    , enabled
from cte_fin

order by rank_order asc, rank_order_tiebreaker asc, depth asc
;