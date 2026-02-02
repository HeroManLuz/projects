----The main table for forecasting
CREATE OR REPLACE TABLE `project_id.analytics.benchmark` AS
----===========
SELECT user_id, reg_date, group_id, country, region, lifetime, cumulative_value, avg_time, avg_special_interaction, avg_special_duration, media_count
FROM(
SELECT s.user_id, s.reg_date, s.group_id, s.country, s.region, s.lifetime, s.cumulative_value, COALESCE(t.avg_time,0) AS avg_time, COALESCE(vp.avg_special_interaction,0) AS avg_special_interaction, COALESCE(vp.avg_special_duration,0) AS avg_special_duration, COALESCE(media_count,0) AS media_count
FROM(
SELECT user_id, reg_date, group_id, country, region, lifetime, SUM(amount_value) OVER(PARTITION BY user_id ORDER BY lifetime) AS cumulative_value
FROM(
SELECT user_id, reg_date, group_id, country, region, lifetime, SUM(amount) AS amount_value
FROM(
SELECT s.user_id, reg_date, group_id, country, region, ub.created_at, TIMESTAMP_DIFF(TIMESTAMP(ub.created_at), TIMESTAMP(s.reg_date),DAY) AS lifetime, amount
FROM(
SELECT s.user_id, s.created_at AS reg_date, group_id, country, CASE WHEN country IN ('CountryA','CountryB','CountryC','CountryD','CountryE','CountryF','CountryG','CountryH','CountryI','CountryJ',
'CountryK','CountryL','CountryM','CountryN',
'CountryO','CountryP','CountryQ','CountryR',
'CountryS','CountryT','CountryU','CountryV',
'CountryW','CountryX','CountryY','CountryZ','CountryAA','CountryAB','CountryAC','CountryAD') THEN 'RegionA' ELSE 'RegionB' END AS region
FROM project_id.mysql.user_role s
LEFT JOIN project_id.mysql.user_ip_country ip ON s.user_id=ip.user_id
LEFT JOIN project_id.mysql.countries_continents cont ON cont.iso_2=ip.country_code
WHERE DATE(s.created_at) BETWEEN CURRENT_DATE()-9 AND CURRENT_DATE()-1) s
INNER JOIN mysql.transaction_history ub ON ub.user_id=s.user_id AND TIMESTAMP_DIFF(TIMESTAMP(ub.created_at), TIMESTAMP(s.reg_date),DAY)<=8
WHERE JSON_VALUE(description,'$.action') NOT IN('deduction','referral_add') AND DATE(ub.created_at) BETWEEN CURRENT_DATE()-10 AND CURRENT_DATE()-1)
GROUP BY ALL
ORDER BY 1,6)) s
-----------average time per day
LEFT JOIN (
SELECT user_id, reg_date, group_id, country, region, lifetime, AVG(minutes) OVER (PARTITION BY user_id ORDER BY lifetime) AS avg_time
FROM(
SELECT user_id, reg_date, group_id, country, region, lifetime, SUM(minutes) AS minutes
FROM(
SELECT user_id, reg_date, group_id, country, region, lifetime, SUM(minutes) AS minutes
FROM(
SELECT user_id, reg_date, group_id, country, region, session_id, min_event_time, lifetime_min AS lifetime, minutes
FROM(
SELECT user_id, reg_date, group_id, country, region, session_id, min_event_time, max_event_time, TIMESTAMP_DIFF(max_event_time, min_event_time, MINUTE) AS minutes, TIMESTAMP_DIFF(min_event_time, TIMESTAMP(reg_date),DAY) AS lifetime_min, TIMESTAMP_DIFF(max_event_time, TIMESTAMP(reg_date), DAY) AS lifetime_max
FROM(
SELECT user_id, reg_date, group_id, country, region, session_id, MIN(event_time) AS min_event_time, MAX(event_time) AS max_event_time
FROM(
SELECT s.user_id, reg_date, group_id, country, region, event_time, event_type, session_id
FROM(
SELECT s.user_id, s.created_at AS reg_date, group_id, country, CASE WHEN country IN ('CountryA','CountryB','CountryC','CountryD','CountryE','CountryF','CountryG','CountryH','CountryI','CountryJ',
'CountryK','CountryL','CountryM','CountryN',
'CountryO','CountryP','CountryQ','CountryR',
'CountryS','CountryT','CountryU','CountryV',
'CountryW','CountryX','CountryY','CountryZ','CountryAA','CountryAB','CountryAC','CountryAD') THEN 'RegionA' ELSE 'RegionB' END AS region
FROM project_id.mysql.user_role s
LEFT JOIN project_id.mysql.user_ip_country ip ON s.user_id=ip.user_id
LEFT JOIN project_id.mysql.countries_continents cont ON cont.iso_2=ip.country_code
WHERE DATE(s.created_at) BETWEEN CURRENT_DATE()-10 AND CURRENT_DATE()-1) s
LEFT JOIN (
SELECT user_id, event_time, event_type, session_id
FROM project_id.event_tracking.event_android_new
WHERE DATE(event_time) BETWEEN CURRENT_DATE()-10 AND CURRENT_DATE()-1 AND event_date BETWEEN CURRENT_DATE()-13 AND CURRENT_DATE()-1
UNION ALL
SELECT user_id, event_time, event_type, session_id,
FROM project_id.event_tracking.event_ios_new
WHERE DATE(event_time) BETWEEN CURRENT_DATE()-10 AND CURRENT_DATE()-1 AND event_date BETWEEN CURRENT_DATE()-13 AND CURRENT_DATE()-1
UNION ALL
SELECT SAFE_CAST(user_id AS INT64), event_time, event_type, session_id
FROM project_id.event_tracking.event_web_new
WHERE DATE(event_time) BETWEEN CURRENT_DATE()-10 AND CURRENT_DATE()-1 AND event_date BETWEEN CURRENT_DATE()-13 AND CURRENT_DATE()-1
UNION ALL
SELECT SAFE_CAST(user_id AS INT64), event_time, event_type, session_id
FROM project_id.event_tracking.event_web_special
WHERE DATE(event_time) BETWEEN CURRENT_DATE()-10 AND CURRENT_DATE()-1 AND event_date BETWEEN CURRENT_DATE()-13 AND CURRENT_DATE()-1
) ampl ON ampl.user_id=s.user_id AND TIMESTAMP_DIFF(event_time, TIMESTAMP(reg_date), DAY)<=8
WHERE session_id!=-1)
GROUP BY ALL
ORDER BY 1)
ORDER BY 1)
WHERE lifetime_min=lifetime_max)
GROUP BY ALL
UNION ALL
SELECT user_id, reg_date, group_id, country, region, lifetime_min AS lifetime, SUM(minutes_min) AS minutes
FROM(
SELECT user_id, reg_date, group_id, country, region, session_id, min_event_time, lifetime_min, TIMESTAMP_DIFF(TIMESTAMP(lifetime_min_add),min_event_time, MINUTE) AS minutes_min, max_event_time, lifetime_max, TIMESTAMP_DIFF(max_event_time, TIMESTAMP(lifetime_min_add),MINUTE) AS minutes_max, lifetime_min_add
FROM(
SELECT user_id, reg_date, group_id, country, region, session_id, min_event_time, lifetime_min, max_event_time, lifetime_max, minutes, DATE_ADD(reg_date, INTERVAL lifetime_max DAY) AS lifetime_min_add
FROM(
SELECT user_id, reg_date, group_id, country, region, session_id, min_event_time, max_event_time, TIMESTAMP_DIFF(max_event_time, min_event_time, MINUTE) AS minutes, TIMESTAMP_DIFF(min_event_time, TIMESTAMP(reg_date),DAY) AS lifetime_min, TIMESTAMP_DIFF(max_event_time, TIMESTAMP(reg_date), DAY) AS lifetime_max
FROM(
SELECT user_id, reg_date, group_id, country, region, session_id, MIN(event_time) AS min_event_time, MAX(event_time) AS max_event_time
FROM(
SELECT s.user_id, reg_date, group_id, country, region, event_time, event_type, session_id
FROM(
SELECT s.user_id, s.created_at AS reg_date, group_id, country, CASE WHEN country IN ('CountryA','CountryB','CountryC','CountryD','CountryE','CountryF','CountryG','CountryH','CountryI','CountryJ',
'CountryK','CountryL','CountryM','CountryN',
'CountryO','CountryP','CountryQ','CountryR',
'CountryS','CountryT','CountryU','CountryV',
'CountryW','CountryX','CountryY','CountryZ','CountryAA','CountryAB','CountryAC','CountryAD') THEN 'RegionA' ELSE 'RegionB' END AS region
FROM project_id.mysql.user_role s
LEFT JOIN project_id.mysql.user_ip_country ip ON s.user_id=ip.user_id
LEFT JOIN project_id.mysql.countries_continents cont ON cont.iso_2=ip.country_code
WHERE DATE(s.created_at) BETWEEN CURRENT_DATE()-9 AND CURRENT_DATE()-1) s
LEFT JOIN (
SELECT user_id, event_time, event_type, session_id
FROM project_id.event_tracking.event_android_new
WHERE DATE(event_time) BETWEEN CURRENT_DATE()-10 AND CURRENT_DATE()-1 AND event_date BETWEEN CURRENT_DATE()-13 AND CURRENT_DATE()-1
UNION ALL
SELECT user_id, event_time, event_type, session_id,
FROM project_id.event_tracking.event_ios_new
WHERE DATE(event_time) BETWEEN CURRENT_DATE()-10 AND CURRENT_DATE()-1 AND event_date BETWEEN CURRENT_DATE()-13 AND CURRENT_DATE()-1
UNION ALL
SELECT SAFE_CAST(user_id AS INT64), event_time, event_type, session_id
FROM project_id.event_tracking.event_web_new
WHERE DATE(event_time) BETWEEN CURRENT_DATE()-10 AND CURRENT_DATE()-1 AND event_date BETWEEN CURRENT_DATE()-13 AND CURRENT_DATE()-1
UNION ALL
SELECT SAFE_CAST(user_id AS INT64), event_time, event_type, session_id
FROM project_id.event_tracking.event_web_special
WHERE DATE(event_time) BETWEEN CURRENT_DATE()-10 AND CURRENT_DATE()-1 AND event_date BETWEEN CURRENT_DATE()-13 AND CURRENT_DATE()-1
) ampl ON ampl.user_id=s.user_id AND TIMESTAMP_DIFF(event_time, TIMESTAMP(reg_date), DAY)<=8
WHERE session_id!=-1)
GROUP BY ALL
ORDER BY 1)
ORDER BY 1)
WHERE lifetime_min!=lifetime_max AND lifetime_max-lifetime_min<=1
ORDER BY 1)
ORDER BY 1)
GROUP BY ALL
UNION ALL
SELECT user_id, reg_date, group_id, country, region, lifetime_max AS lifetime, SUM(minutes_max) AS minutes
FROM(
SELECT user_id, reg_date, group_id, country, region, session_id, min_event_time, lifetime_min, TIMESTAMP_DIFF(TIMESTAMP(lifetime_min_add),min_event_time, MINUTE) AS minutes_min, max_event_time, lifetime_max, TIMESTAMP_DIFF(max_event_time, TIMESTAMP(lifetime_min_add),MINUTE) AS minutes_max, lifetime_min_add
FROM(
SELECT user_id, reg_date, group_id, country, region, session_id, min_event_time, lifetime_min, max_event_time, lifetime_max, minutes, DATE_ADD(reg_date, INTERVAL lifetime_max DAY) AS lifetime_min_add
FROM(
SELECT user_id, reg_date, group_id, country, region, session_id, min_event_time, max_event_time, TIMESTAMP_DIFF(max_event_time, min_event_time, MINUTE) AS minutes, TIMESTAMP_DIFF(min_event_time, TIMESTAMP(reg_date),DAY) AS lifetime_min, TIMESTAMP_DIFF(max_event_time, TIMESTAMP(reg_date), DAY) AS lifetime_max
FROM(
SELECT user_id, reg_date, group_id, country, region, session_id, MIN(event_time) AS min_event_time, MAX(event_time) AS max_event_time
FROM(
SELECT s.user_id, reg_date, group_id, country, region, event_time, event_type, session_id
FROM(
SELECT s.user_id, s.created_at AS reg_date, group_id, country, CASE WHEN country IN ('CountryA','CountryB','CountryC','CountryD','CountryE','CountryF','CountryG','CountryH','CountryI','CountryJ',
'CountryK','CountryL','CountryM','CountryN',
'CountryO','CountryP','CountryQ','CountryR',
'CountryS','CountryT','CountryU','CountryV',
'CountryW','CountryX','CountryY','CountryZ','CountryAA','CountryAB','CountryAC','CountryAD') THEN 'RegionA' ELSE 'RegionB' END AS region
FROM project_id.mysql.user_role s
LEFT JOIN project_id.mysql.user_ip_country ip ON s.user_id=ip.user_id
LEFT JOIN project_id.mysql.countries_continents cont ON cont.iso_2=ip.country_code
WHERE DATE(s.created_at) BETWEEN CURRENT_DATE()-9 AND CURRENT_DATE()-1) s
LEFT JOIN (
SELECT user_id, event_time, event_type, session_id
FROM project_id.event_tracking.event_android_new
WHERE DATE(event_time) BETWEEN CURRENT_DATE()-10 AND CURRENT_DATE()-1 AND event_date BETWEEN CURRENT_DATE()-13 AND CURRENT_DATE()-1
UNION ALL
SELECT user_id, event_time, event_type, session_id,
FROM project_id.event_tracking.event_ios_new
WHERE DATE(event_time) BETWEEN CURRENT_DATE()-10 AND CURRENT_DATE()-1 AND event_date BETWEEN CURRENT_DATE()-13 AND CURRENT_DATE()-1
UNION ALL
SELECT SAFE_CAST(user_id AS INT64), event_time, event_type, session_id
FROM project_id.event_tracking.event_web_new
WHERE DATE(event_time) BETWEEN CURRENT_DATE()-10 AND CURRENT_DATE()-1 AND event_date BETWEEN CURRENT_DATE()-13 AND CURRENT_DATE()-1
UNION ALL
SELECT SAFE_CAST(user_id AS INT64), event_time, event_type, session_id
FROM project_id.event_tracking.event_web_special
WHERE DATE(event_time) BETWEEN CURRENT_DATE()-10 AND CURRENT_DATE()-1 AND event_date BETWEEN CURRENT_DATE()-13 AND CURRENT_DATE()-1
) ampl ON ampl.user_id=s.user_id AND TIMESTAMP_DIFF(event_time, TIMESTAMP(reg_date), DAY)<=8
WHERE session_id!=-1)
GROUP BY ALL
ORDER BY 1)
ORDER BY 1)
WHERE lifetime_min!=lifetime_max AND lifetime_max-lifetime_min<=1
ORDER BY 1)
ORDER BY 1)
GROUP BY ALL
ORDER BY 1)
GROUP BY ALL)
ORDER BY 1,6
) t ON s.user_id=t.user_id AND t.lifetime=s.lifetime
----interactions
LEFT JOIN (
SELECT user_id, reg_date, group_id, country, region, lifetime, avg_special_interaction, sum_special_duration/sum_special_interaction AS avg_special_duration
FROM(
SELECT user_id, reg_date, group_id, country, region, lifetime, AVG(special_interaction) OVER(PARTITION BY user_id ORDER BY lifetime) AS avg_special_interaction, SUM(special_duration) OVER(PARTITION BY user_id ORDER BY lifetime) AS sum_special_duration, SUM(special_interaction) OVER(PARTITION BY user_id ORDER BY lifetime) AS sum_special_interaction
FROM(
SELECT user_id, reg_date, group_id, country, region, lifetime, COUNT(CASE WHEN interaction_type=2 THEN 1 END) AS special_interaction,SUM(CASE WHEN interaction_type=2 THEN duration END) AS special_duration
FROM(
SELECT s.user_id, reg_date, group_id, country, region, created_at, interaction_type, duration, TIMESTAMP_DIFF(created_at, TIMESTAMP(reg_date),DAY) AS lifetime
FROM(
SELECT s.user_id, s.created_at AS reg_date, group_id, country, CASE WHEN country IN ('CountryA','CountryB','CountryC','CountryD','CountryE','CountryF','CountryG','CountryH','CountryI','CountryJ',
'CountryK','CountryL','CountryM','CountryN',
'CountryO','CountryP','CountryQ','CountryR',
'CountryS','CountryT','CountryU','CountryV',
'CountryW','CountryX','CountryY','CountryZ','CountryAA','CountryAB','CountryAC','CountryAD') THEN 'RegionA' ELSE 'RegionB' END AS region
FROM project_id.mysql.user_role s
LEFT JOIN project_id.mysql.user_ip_country ip ON s.user_id=ip.user_id
LEFT JOIN project_id.mysql.countries_continents cont ON cont.iso_2=ip.country_code
WHERE DATE(s.created_at) BETWEEN CURRENT_DATE()-9 AND CURRENT_DATE()-1) s
LEFT JOIN mysql.activity_log vp ON vp.to_user_id=s.user_id AND TIMESTAMP_DIFF(vp.created_at, TIMESTAMP(s.reg_date), DAY)<=8 AND duration>0
WHERE DATE(vp.created_at) BETWEEN CURRENT_DATE()-10 AND CURRENT_DATE()-1)
GROUP BY 1,2,3,4,5,6
ORDER BY 1,6))
ORDER BY 1,6) vp ON vp.user_id=s.user_id AND vp.lifetime=s.lifetime
----media
LEFT JOIN (
SELECT user_id, reg_date, group_id, country, region, lifetime, COUNT(DISTINCT media) AS media_count
FROM(
SELECT s.user_id, reg_date, group_id, country, region, lifetime, p.created_at, p.name AS media
FROM(
SELECT s.user_id, reg_date, group_id, country, region, lifetime
FROM (
SELECT s.user_id, s.created_at AS reg_date, group_id, country,
CASE WHEN country IN ('CountryA','CountryB','CountryC','CountryD','CountryE','CountryF','CountryG','CountryH','CountryI','CountryJ',
'CountryK','CountryL','CountryM','CountryN',
'CountryO','CountryP','CountryQ','CountryR',
'CountryS','CountryT','CountryU','CountryV',
'CountryW','CountryX','CountryY','CountryZ','CountryAA','CountryAB','CountryAC','CountryAD') THEN 'RegionA'
ELSE 'RegionB'
END AS region
FROM project_id.mysql.user_role s
LEFT JOIN project_id.mysql.user_ip_country ip ON s.user_id = ip.user_id
LEFT JOIN project_id.mysql.countries_continents cont ON cont.iso_2 = ip.country_code
WHERE DATE(s.created_at) BETWEEN CURRENT_DATE() - 9 AND CURRENT_DATE() - 1
) s
CROSS JOIN UNNEST(GENERATE_ARRAY(0, 8)) AS lifetime
ORDER BY 1,6) s
LEFT JOIN mysql.user_media p ON p.user_id=s.user_id AND TIMESTAMP_DIFF(TIMESTAMP(p.created_at), TIMESTAMP(s.reg_date), DAY)<=lifetime
WHERE approval_status IN (1,2,3) AND DATE(p.created_at) BETWEEN CURRENT_DATE()-10 AND CURRENT_DATE()-1
ORDER BY 1,6)
GROUP BY ALL
ORDER BY 1,6
) p ON p.user_id=s.user_id AND p.lifetime=s.lifetime)
QUALIFY ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY lifetime DESC)=1
ORDER BY 1,6;
---------Benchmark results
TRUNCATE TABLE project_id.analytics.benchmark_results;
INSERT INTO project_id.analytics.benchmark_results
SELECT user_id, reg_date, group_id, country, region, lifetime, cumulative_value, avg_time, avg_special_interaction, avg_special_duration, media_count, predicted_outcome, predicted_outcome_probs[OFFSET(0)].prob AS probability_outcome,
CASE WHEN predicted_outcome = 0 THEN 'Negative' ELSE 'Positive' END AS category
FROM
ML.PREDICT (MODEL `project_id.analytics.outcome_model`,
(SELECT *
FROM `project_id.analytics.benchmark`)
)
ORDER BY user_id, lifetime;
---Archive results
DELETE FROM project_id.analytics.benchmark_archive WHERE archive_date=CURRENT_DATE();
INSERT INTO project_id.analytics.benchmark_archive
SELECT CURRENT_DATE() AS archive_date, user_id, reg_date, group_id, country, region, lifetime, predicted_outcome
FROM project_id.analytics.benchmark_results
ORDER BY user_id, lifetime;
--- Archive for filtering predict-fact lifetime
TRUNCATE TABLE project_id.analytics.benchmark_archive_lifetime;
INSERT INTO project_id.analytics.benchmark_archive_lifetime
SELECT *, TIMESTAMP_DIFF(TIMESTAMP(archive_date), TIMESTAMP(reg_date),DAY) AS lifetime_archive
FROM project_id.analytics.benchmark_archive
WHERE TIMESTAMP_DIFF(TIMESTAMP(archive_date), TIMESTAMP(reg_date),DAY) IN(0,3,6) AND DATE(reg_date)>='2026-01-01'
ORDER BY user_id, reg_date;
---Predict fact
CREATE OR REPLACE TABLE `project_id.analytics.benchmark_predict_fact` AS
SELECT archive_date, l.user_id, l.reg_date, group_id, country, region, lifetime_archive, predicted_outcome, w.outcome AS fact_outcome
FROM project_id.analytics.benchmark_archive_lifetime l
LEFT JOIN (
SELECT user_id, reg_date, CASE WHEN amount>=30000 THEN 1 ELSE 0 END AS outcome
FROM(
SELECT user_id, reg_date, SUM(amount) AS amount
FROM(
SELECT s.user_id, reg_date, created_at, amount,
FROM(
SELECT s.user_id, s.created_at AS reg_date
FROM project_id.mysql.user_role s
WHERE DATE(s.created_at) BETWEEN CURRENT_DATE()-9 AND CURRENT_DATE()-1) s
LEFT JOIN mysql.transaction_history ub ON ub.user_id=s.user_id AND TIMESTAMP_DIFF(TIMESTAMP(ub.created_at), TIMESTAMP(s.reg_date), DAY)<=8
WHERE JSON_VALUE(description,'$.action') NOT IN('deduction','referral_add'))
GROUP BY ALL
ORDER BY 1)
ORDER BY 1
) w ON w.user_id=l.user_id
WHERE DATE(l.reg_date) BETWEEN '2026-01-01' AND CURRENT_DATE()-9;
----Reporting Tool Benchmark
CREATE OR REPLACE TABLE project_id.analytics.excel_benchmark AS
SELECT s.user_id AS `User Id`, DATE(reg_date) AS `Reg Date`, group_id AS `Group Id`, low_value_new AS `New users low value`, low_time_new AS `New users low time on product`, low_priority_new AS `New users with low priority`, low_priority_old AS `Old users with low priority`, CASE WHEN media_count<5 THEN '+' ELSE '-' END AS `Users with less then 5 added media`, CASE WHEN avg_rating <=3.7 THEN '+' ELSE '-' END AS `Users with avg rating less then 3_7`, CASE WHEN restrictions>=3 THEN '+' ELSE '-' END AS `Users with more then 3 restrictions`, CASE WHEN k.user_id IS NOT NULL THEN '+' ELSE '-' END AS `Users with no value last 7 days`
FROM(
SELECT s.user_id, s.reg_date, s.group_id, CASE WHEN amount<=10000 THEN '+' ELSE '-' END AS low_value_new,
 CASE WHEN avg_time<=210 OR avg_time IS NULL THEN '+' ELSE '-' END AS low_time_new,
CASE WHEN priority<=200 THEN '+' ELSE '-' END AS low_priority_new, '-' AS low_priority_old
FROM(
SELECT user_id, reg_date, group_id, COALESCE(SUM(amount),0) AS amount
FROM(
SELECT s.user_id, reg_date, group_id, ub.created_at, amount
FROM(
SELECT s.user_id, s.created_at AS reg_date, group_id,
FROM project_id.mysql.user_role s
INNER JOIN (SELECT DISTINCT user_id
FROM project_id.analytics.user_actions
WHERE segmentation='1+') h ON s.user_id=h.user_id
WHERE DATE(s.created_at) BETWEEN CURRENT_DATE()-14 AND CURRENT_DATE()-1) s
LEFT JOIN mysql.transaction_history ub ON ub.user_id=s.user_id AND TIMESTAMP_DIFF(TIMESTAMP(ub.created_at),TIMESTAMP(reg_date),DAY)<=8 AND JSON_VALUE(description,'$.action') NOT IN('deduction','referral_add')
ORDER BY ub.created_at
)
GROUP BY ALL
ORDER BY 1
) s
--time on a product
LEFT JOIN (
SELECT user_id, reg_date, group_id, COALESCE(AVG(minutes),0) AS avg_time
FROM(
SELECT user_id, reg_date,group_id, date, SUM(minutes)AS minutes
FROM(
SELECT user_id, reg_date, group_id, date, minutes
FROM(
SELECT user_id, reg_date, group_id, DATE(min_event_time) AS date, SUM(minutes) AS minutes
FROM(
SELECT user_id, reg_date, group_id, session_id, min_event_time, max_event_time, TIMESTAMP_DIFF(max_event_time, min_event_time, MINUTE) AS minutes
FROM(
SELECT user_id, reg_date, group_id, session_id, MIN(event_time) AS min_event_time, MAX(event_time) AS max_event_time
FROM(
SELECT c.user_id, reg_date, group_id, event_time, session_id, event_type
FROM (
SELECT s.user_id, s.created_at AS reg_date, group_id,
FROM project_id.mysql.user_role s
INNER JOIN (SELECT DISTINCT user_id
FROM project_id.analytics.user_actions
WHERE segmentation='1+') h ON s.user_id=h.user_id
WHERE DATE(s.created_at) BETWEEN CURRENT_DATE()-14 AND CURRENT_DATE()-1
) c
LEFT JOIN (SELECT user_id, event_time, event_type, session_id
FROM project_id.event_tracking.event_android_new
WHERE DATE(event_time) BETWEEN CURRENT_DATE()-14 AND CURRENT_DATE()-1 AND event_date BETWEEN CURRENT_DATE()-17 AND CURRENT_DATE()-1
UNION ALL
SELECT user_id, event_time, event_type, session_id,
FROM project_id.event_tracking.event_ios_new
WHERE DATE(event_time) BETWEEN CURRENT_DATE()-14 AND CURRENT_DATE()-1 AND event_date BETWEEN CURRENT_DATE()-17 AND CURRENT_DATE()-1
UNION ALL
SELECT SAFE_CAST(user_id AS INT64), event_time, event_type, session_id
FROM project_id.event_tracking.event_web_new
WHERE DATE(event_time) BETWEEN CURRENT_DATE()-14 AND CURRENT_DATE()-1 AND event_date BETWEEN CURRENT_DATE()-17 AND CURRENT_DATE()-1
UNION ALL
SELECT SAFE_CAST(user_id AS INT64), event_time, event_type, session_id
FROM project_id.event_tracking.event_web_special
WHERE DATE(event_time) BETWEEN CURRENT_DATE()-14 AND CURRENT_DATE()-1 AND event_date BETWEEN CURRENT_DATE()-17 AND CURRENT_DATE()-1) ampl ON ampl.user_id=c.user_id
WHERE session_id!=-1
ORDER BY 1,4)
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4
)
WHERE DATE(max_event_time)!=DATE_ADD(DATE(min_event_time), INTERVAL 1 DAY)
)
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4)
UNION ALL(
SELECT user_id, reg_date, group_id, DATE(min_event_time) AS date, SUM(minute_1) AS minutes
FROM(
SELECT user_id, reg_date, group_id, min_event_time, TIMESTAMP_DIFF(midnight, min_event_time, MINUTE) AS minute_1, max_event_time, TIMESTAMP_DIFF(max_event_time,midnight,MINUTE) AS minute_2
FROM(
SELECT user_id, reg_date, group_id, session_id, min_event_time,TIMESTAMP(DATE(DATE_ADD(min_event_time, INTERVAL 1 DAY))) AS midnight, max_event_time
FROM(
SELECT user_id, reg_date, group_id, session_id, MIN(event_time) AS min_event_time, MAX(event_time) AS max_event_time
FROM(
SELECT c.user_id, reg_date, group_id, event_time, session_id, event_type
FROM (
SELECT s.user_id, s.created_at AS reg_date, group_id,
FROM project_id.mysql.user_role s
INNER JOIN (SELECT DISTINCT user_id
FROM project_id.analytics.user_actions
WHERE segmentation='1+') h ON s.user_id=h.user_id
WHERE DATE(s.created_at) BETWEEN CURRENT_DATE()-14 AND CURRENT_DATE()-1
) c
LEFT JOIN (SELECT user_id, event_time, event_type, session_id
FROM project_id.event_tracking.event_android_new
WHERE DATE(event_time) BETWEEN CURRENT_DATE()-14 AND CURRENT_DATE()-1 AND event_date BETWEEN CURRENT_DATE()-17 AND CURRENT_DATE()-1
UNION ALL
SELECT user_id, event_time, event_type, session_id,
FROM project_id.event_tracking.event_ios_new
WHERE DATE(event_time) BETWEEN CURRENT_DATE()-14 AND CURRENT_DATE()-1 AND event_date BETWEEN CURRENT_DATE()-17 AND CURRENT_DATE()-1
UNION ALL
SELECT SAFE_CAST(user_id AS INT64), event_time, event_type, session_id
FROM project_id.event_tracking.event_web_new
WHERE DATE(event_time) BETWEEN CURRENT_DATE()-14 AND CURRENT_DATE()-1 AND event_date BETWEEN CURRENT_DATE()-17 AND CURRENT_DATE()-1
UNION ALL
SELECT SAFE_CAST(user_id AS INT64), event_time, event_type, session_id
FROM project_id.event_tracking.event_web_special
WHERE DATE(event_time) BETWEEN CURRENT_DATE()-14 AND CURRENT_DATE()-1 AND event_date BETWEEN CURRENT_DATE()-17 AND CURRENT_DATE()-1) ampl ON ampl.user_id=c.user_id
WHERE session_id!=-1
ORDER BY 1,4)
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4
)
WHERE DATE(max_event_time)=DATE_ADD(DATE(min_event_time), INTERVAL 1 DAY)
))
GROUP BY 1,2,3,4
UNION ALL
SELECT user_id, reg_date, group_id, DATE(max_event_time) AS date, SUM(minute_2) AS minutes
FROM(
SELECT user_id, reg_date, group_id, min_event_time, TIMESTAMP_DIFF(midnight, min_event_time, MINUTE) AS minute_1, max_event_time, TIMESTAMP_DIFF(max_event_time,midnight,MINUTE) AS minute_2
FROM(
SELECT user_id, reg_date, group_id, session_id, min_event_time,TIMESTAMP(DATE(DATE_ADD(min_event_time, INTERVAL 1 DAY))) AS midnight, max_event_time
FROM(
SELECT user_id, reg_date, group_id, session_id, MIN(event_time) AS min_event_time, MAX(event_time) AS max_event_time
FROM(
SELECT c.user_id, reg_date, group_id, event_time, session_id, event_type
FROM (
SELECT s.user_id, s.created_at AS reg_date, group_id,
FROM project_id.mysql.user_role s
INNER JOIN (SELECT DISTINCT user_id
FROM project_id.analytics.user_actions
WHERE segmentation='1+') h ON s.user_id=h.user_id
WHERE DATE(s.created_at) BETWEEN CURRENT_DATE()-14 AND CURRENT_DATE()-1
) c
LEFT JOIN (SELECT user_id, event_time, event_type, session_id
FROM project_id.event_tracking.event_android_new
WHERE DATE(event_time) BETWEEN CURRENT_DATE()-14 AND CURRENT_DATE()-1 AND event_date BETWEEN CURRENT_DATE()-17 AND CURRENT_DATE()-1
UNION ALL
SELECT user_id, event_time, event_type, session_id,
FROM project_id.event_tracking.event_ios_new
WHERE DATE(event_time) BETWEEN CURRENT_DATE()-14 AND CURRENT_DATE()-1 AND event_date BETWEEN CURRENT_DATE()-17 AND CURRENT_DATE()-1
UNION ALL
SELECT SAFE_CAST(user_id AS INT64), event_time, event_type, session_id
FROM project_id.event_tracking.event_web_new
WHERE DATE(event_time) BETWEEN CURRENT_DATE()-14 AND CURRENT_DATE()-1 AND event_date BETWEEN CURRENT_DATE()-17 AND CURRENT_DATE()-1
UNION ALL
SELECT SAFE_CAST(user_id AS INT64), event_time, event_type, session_id
FROM project_id.event_tracking.event_web_special
WHERE DATE(event_time) BETWEEN CURRENT_DATE()-14 AND CURRENT_DATE()-1 AND event_date BETWEEN CURRENT_DATE()-17 AND CURRENT_DATE()-1) ampl ON ampl.user_id=c.user_id
WHERE session_id!=-1
ORDER BY 1,4)
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4
)
WHERE DATE(max_event_time)=DATE_ADD(DATE(min_event_time), INTERVAL 1 DAY)
))
GROUP BY 1,2,3,4
ORDER BY 1,2))
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4)
GROUP BY ALL
ORDER BY 1,2,3,4
) ampl ON s.user_id=ampl.user_id
--priority
LEFT JOIN mysql.user_role h ON s.user_id=h.user_id
UNION ALL
SELECT s.user_id, s.reg_date, s.group_id, '-' AS low_value_new,
'-' AS low_time_new,
'-' AS low_priority_new, CASE WHEN priority<=200 THEN '+' ELSE '-' END AS low_priority_old
FROM(
SELECT user_id, reg_date, group_id, COALESCE(SUM(amount),0) AS amount
FROM(
SELECT s.user_id, reg_date, group_id, ub.created_at, amount
FROM(
SELECT s.user_id, s.created_at AS reg_date, group_id,
FROM project_id.mysql.user_role s
INNER JOIN (SELECT DISTINCT user_id
FROM project_id.analytics.user_actions
WHERE segmentation='1+') h ON s.user_id=h.user_id
WHERE DATE(s.created_at) < CURRENT_DATE()-14) s
LEFT JOIN mysql.transaction_history ub ON ub.user_id=s.user_id AND DATE(ub.created_at) BETWEEN CURRENT_DATE()-61 AND CURRENT_DATE()-1 AND JSON_VALUE(description,'$.action') NOT IN('deduction','referral_add')
)
GROUP BY ALL
HAVING amount>=1000
ORDER BY 1) s
LEFT JOIN mysql.user_role h ON s.user_id=h.user_id
ORDER BY 1
) s
--media
LEFT JOIN (
SELECT user_id, COUNT(DISTINCT name) AS media_count
FROM mysql.user_media
WHERE approval_status IN(1,2,3)
GROUP BY 1
ORDER BY 1
) p ON s.user_id=p.user_id
--rating
LEFT JOIN (
SELECT target_user_id, AVG(rating) AS avg_rating
FROM project_id.mysql.user_feedback
GROUP BY 1
ORDER BY 1
) f ON s.user_id=f.target_user_id
--restrictions
LEFT JOIN (
SELECT target_id, restrictions, user_id
FROM(
SELECT target_id, COUNT(DISTINCT end_time) AS restrictions
FROM(
SELECT *
FROM mysql.user_restriction
WHERE target_id NOT IN(
SELECT DISTINCT target_id
FROM project_id.mysql.user_restriction
WHERE restriction_place LIKE '%deviceId%')
AND DATE(end_time) BETWEEN CURRENT_DATE()-61 AND CURRENT_DATE()-1)
GROUP BY 1
ORDER BY 1) f
LEFT JOIN `mysql.user_role` s ON s.id=f.target_id
) b ON b.user_id=s.user_id
--without last 7 days value user
LEFT JOIN (
SELECT DISTINCT user_id
FROM(
SELECT s.user_id, reg_date, group_id, ub.created_at, ub.amount
FROM(
SELECT user_id, reg_date, group_id, COALESCE(SUM(amount),0) AS amount
FROM(
SELECT s.user_id, reg_date, group_id, ub.created_at, amount
FROM(
SELECT s.user_id, s.created_at AS reg_date, group_id,
FROM project_id.mysql.user_role s
INNER JOIN (SELECT DISTINCT user_id
FROM project_id.analytics.user_actions
WHERE segmentation='1+') h ON s.user_id=h.user_id
WHERE DATE(s.created_at) < CURRENT_DATE()-14) s
LEFT JOIN mysql.transaction_history ub ON ub.user_id=s.user_id AND DATE(ub.created_at) BETWEEN CURRENT_DATE()-61 AND CURRENT_DATE()-1 AND JSON_VALUE(description,'$.action') NOT IN('deduction','referral_add')
)
GROUP BY ALL
HAVING amount>=1000
ORDER BY 1) s
LEFT JOIN mysql.transaction_history ub ON ub.user_id=s.user_id AND DATE(ub.created_at) BETWEEN CURRENT_DATE()-7 AND CURRENT_DATE()-1 AND JSON_VALUE(description,'$.action') NOT IN('deduction','referral_add'))
WHERE created_at IS NULL
) k ON k.user_id=s.user_id
ORDER BY s.user_id;
