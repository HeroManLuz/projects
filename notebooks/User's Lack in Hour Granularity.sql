CREATE OR REPLACE TABLE project_id.provider_lack.provider_lack_hour AS
-----hourly providers lack
WITH basic_data AS (SELECT providers.date, providers.type_day, providers.hour,providers.country, providers.providers, callers_0.callers_0, callers_0.new_callers
FROM(
SELECT date, type_day, hour, country, COUNT(DISTINCT provider_id) AS providers
FROM project_id.provider_lack.base_data
WHERE duration>0 AND DATE(date) BETWEEN '2025-01-01' AND CURRENT_DATE()-1
GROUP BY 1,2,3,4) providers
--callers duration more than 0 seconds
LEFT JOIN(
SELECT date, type_day,hour, country, COUNT(DISTINCT caller_id) AS callers_0, COUNT(DISTINCT CASE WHEN DATE(created_at)=DATE(created_at) THEN caller_id END ) AS new_callers
FROM project_id.provider_lack.base_data bd
LEFT JOIN project_id.mysql.user_info u ON u.id=bd.caller_id
WHERE duration>0 AND DATE(date) BETWEEN '2025-01-01' AND CURRENT_DATE()-1
GROUP BY 1,2,3,4
) callers_0 ON providers.date=callers_0.date AND providers.type_day=callers_0.type_day AND providers.hour=callers_0.hour AND providers.country=callers_0.country),
---active providers (duration more than 0 second in hour)
active_callers AS (
SELECT date, type_day, hour, country, provider_id, duration
FROM project_id.provider_lack.base_data
WHERE DATE(date) BETWEEN '2025-01-01' AND CURRENT_DATE()-1
GROUP BY ALL
HAVING duration>0
),
----callers with more than 2 tries
more_than_2 AS(
SELECT date, type_day, hour, caller_id
FROM(
SELECT vp.date, vp.type_day, vp.hour,
caller_id, created_at
FROM project_id.provider_lack.base_data vp
LEFT JOIN active_callers au ON au.provider_id = vp.provider_id
AND au.date = vp.date
AND au.type_day = vp.type_day
AND au.hour=vp.hour
WHERE au.provider_id IS NOT NULL AND DATE(vp.date) BETWEEN '2025-01-01' AND CURRENT_DATE()-1
GROUP BY ALL)
GROUP BY ALL
HAVING COUNT(caller_id)>=2
),
----base data for further actions
base_data AS (
SELECT
vp.date,vp.type_day, vp.hour, vp.country,
vp.caller_id,
vp.duration, vp.created_at, vp.provider_id
FROM project_id.provider_lack.base_data vp
LEFT JOIN active_callers au ON au.provider_id = vp.provider_id
AND au.date = vp.date
AND au.type_day = vp.type_day
AND au.hour=vp.hour
LEFT JOIN more_than_2 m2 ON m2.caller_id = vp.caller_id
AND m2.date = vp.date
AND m2.type_day = vp.type_day
AND m2.hour=vp.hour
WHERE au.provider_id IS NOT NULL
AND m2.caller_id IS NOT NULL
AND DATE(vp.date) BETWEEN '2025-01-01' AND CURRENT_DATE()-1
GROUP BY ALL
),
----zero activity callers and zero_activity_callers_60
zero_activity_callers AS(
SELECT zero_callers.date, zero_callers.type_day, zero_callers.hour, zero_callers.country, zero_activity_callers, COALESCE(new_zero_activity_callers_60,0) AS new_zero_activity_callers_60
FROM(SELECT date, type_day, hour, country, COUNT(DISTINCT caller_id) AS zero_activity_callers
FROM(
SELECT bd.date,bd.type_day,bd.hour,bd.country,bd.caller_id
FROM base_data bd
WHERE bd.duration=0)
GROUP BY all) zero_callers
LEFT JOIN (
SELECT date, type_day, hour, initial_country AS country, COUNT(DISTINCT caller_id) AS new_zero_activity_callers_60
FROM(
SELECT bd.date,bd.type_day,bd.hour,bd.caller_id,bd.created_at,bd.country AS initial_country,
FROM base_data bd
LEFT JOIN base_data next_hour
ON bd.caller_id = next_hour.caller_id
AND TIMESTAMP_DIFF(TIMESTAMP(next_hour.created_at), TIMESTAMP(bd.created_at), SECOND) BETWEEN 1 AND 3600
AND next_hour.duration = 0
LEFT JOIN project_id.mysql.user_info u ON u.id=bd.caller_id AND DATE(created_at)=bd.date
WHERE bd.duration = 0 AND next_hour.caller_id IS NOT NULL AND u.id IS NOT NULL
GROUP BY all
UNION DISTINCT
SELECT bd.date,bd.type_day,bd.hour,bd.caller_id,bd.created_at,bd.country AS initial_country
FROM base_data bd
LEFT JOIN base_data next_hour
ON bd.caller_id = next_hour.caller_id
AND TIMESTAMP_DIFF(TIMESTAMP(next_hour.created_at), TIMESTAMP(bd.created_at), SECOND) BETWEEN 1 AND 3600
AND next_hour.caller_id IS NULL
LEFT JOIN project_id.mysql.user_info u ON u.id=bd.caller_id AND DATE(created_at)=bd.date
WHERE bd.duration = 0 AND next_hour.caller_id IS NULL AND u.id IS NOT NULL
GROUP BY all)
GROUP BY all
) new_zero_60 ON new_zero_60.date=zero_callers.date AND new_zero_60.hour=zero_callers.hour AND new_zero_60.country=zero_callers.country
ORDER BY date DESC, hour ASC),
--zero_experience_callers
zero_experience_callers AS (
SELECT date, type_day, hour, country, COUNT(DISTINCT caller_id) AS new_zero_experience_callers
FROM(
SELECT date, type_day, hour, country, caller_id
FROM(
SELECT bd.date, type_day, bd.hour, bd.caller_id, country
FROM(
SELECT bd.date,bd.type_day,bd.hour,bd.caller_id,bd.created_at, bd.country
FROM base_data bd
LEFT JOIN(
SELECT date, type_day, hour, initial_country AS country, caller_id
FROM(
SELECT bd.date,bd.type_day,bd.hour,bd.caller_id,bd.created_at, bd.country AS initial_country
FROM base_data bd
LEFT JOIN base_data next_hour
ON bd.caller_id = next_hour.caller_id
AND TIMESTAMP_DIFF(TIMESTAMP(next_hour.created_at), TIMESTAMP(bd.created_at), SECOND) BETWEEN 1 AND 3600
AND next_hour.duration = 0
LEFT JOIN project_id.mysql.user_info u ON u.id=bd.caller_id AND DATE(created_at)=bd.date
WHERE bd.duration = 0 AND next_hour.caller_id IS NOT NULL AND u.id IS NOT NULL
GROUP BY all
UNION DISTINCT
SELECT bd.date,bd.type_day,bd.hour, bd.caller_id, bd.created_at, bd.country AS initial_country
FROM base_data bd
LEFT JOIN base_data next_hour
ON bd.caller_id = next_hour.caller_id
AND TIMESTAMP_DIFF(TIMESTAMP(next_hour.created_at), TIMESTAMP(bd.created_at), SECOND) BETWEEN 1 AND 3600
AND next_hour.caller_id IS NULL
LEFT JOIN project_id.mysql.user_info u ON u.id=bd.caller_id AND DATE(created_at)=bd.date
WHERE bd.duration = 0 AND next_hour.caller_id IS NULL AND u.id IS NOT NULL
GROUP BY all)
GROUP BY all) new_zero_activity_callers ON new_zero_activity_callers.date=bd.date AND new_zero_activity_callers.hour=bd.hour AND new_zero_activity_callers.caller_id=bd.caller_id AND new_zero_activity_callers.country=bd.country
WHERE bd.duration=0 AND new_zero_activity_callers.caller_id IS NOT NULL) bd
LEFT JOIN(
SELECT DATE(event_time) AS date, EXTRACT(HOUR FROM event_time) AS hour, EXTRACT(MINUTE FROM event_time) AS minute,caller_id
FROM project_id.provider_lack.provider_lack_event_view
WHERE DATE(event_time) BETWEEN '2025-01-01' AND CURRENT_DATE()-1
GROUP BY all
ORDER BY date, hour ASC
) event ON event.caller_id=bd.caller_id AND event.date=bd.date AND event.hour=bd.hour AND event.minute=EXTRACT(MINUTE FROM bd.created_at)
WHERE event.caller_id IS NOT NULL
GROUP BY all)
GROUP BY all
UNION DISTINCT
SELECT date, type_day, hour, country AS country, caller_id
FROM(
SELECT bd.date,bd.type_day, bd.hour, bd.caller_id, bd.created_at,bd.country
FROM base_data bd
LEFT JOIN project_id.mysql.user_info u ON u.id=bd.caller_id AND DATE(created_at)=bd.date
LEFT JOIN (
SELECT date, caller_id
FROM(
SELECT date, caller_id, SUM(duration) AS duration
FROM base_data
GROUP BY all)
WHERE duration=0
) caller_0 ON caller_0.caller_id=bd.caller_id AND caller_0.date=bd.date
WHERE bd.duration = 0 AND u.id IS NOT NULL AND caller_0.caller_id IS NOT NULL
GROUP BY all)
GROUP BY all)
GROUP BY all
ORDER BY date DESC, hour ASC),
----------------------zero providers for correlation
zero_providers AS (SELECT date, type_day, hour, country, COUNT(DISTINCT caller_id) AS new_zero_final_callers, COUNT(DISTINCT provider_id) AS zero_providers
FROM(
SELECT bd.date, bd.type_day, bd.hour, bd.country, bd.caller_id, bd.provider_id
FROM(
SELECT bd.date, bd.type_day, bd.hour, bd.caller_id, bd.created_at, bd.country, bd.provider_id
FROM base_data bd
LEFT JOIN(
SELECT date, type_day, hour, initial_country AS country, caller_id
FROM(
SELECT bd.date,bd.type_day,bd.hour,bd.caller_id,bd.created_at, bd.country AS initial_country
FROM base_data bd
LEFT JOIN base_data next_hour
ON bd.caller_id = next_hour.caller_id
AND TIMESTAMP_DIFF(TIMESTAMP(next_hour.created_at), TIMESTAMP(bd.created_at), SECOND) BETWEEN 1 AND 3600
AND next_hour.duration = 0
LEFT JOIN project_id.mysql.user_info u ON u.id=bd.caller_id AND DATE(created_at)=bd.date
WHERE bd.duration = 0 AND next_hour.caller_id IS NOT NULL AND u.id IS NOT NULL
GROUP BY all
UNION DISTINCT
SELECT bd.date,bd.type_day,bd.hour,bd.caller_id,bd.created_at, bd.country AS initial_country
FROM base_data bd
LEFT JOIN base_data next_hour
ON bd.caller_id = next_hour.caller_id
AND TIMESTAMP_DIFF(TIMESTAMP(next_hour.created_at), TIMESTAMP(bd.created_at), SECOND) BETWEEN 1 AND 3600
AND next_hour.caller_id IS NULL
LEFT JOIN project_id.mysql.user_info u ON u.id=bd.caller_id AND DATE(created_at)=bd.date
WHERE bd.duration = 0 AND next_hour.caller_id IS NULL AND u.id IS NOT NULL
GROUP BY all)
GROUP BY all) new_zero_activity_callers ON new_zero_activity_callers.date=bd.date AND new_zero_activity_callers.hour=bd.hour AND new_zero_activity_callers.caller_id=bd.caller_id AND new_zero_activity_callers.country=bd.country
WHERE bd.duration=0 AND new_zero_activity_callers.caller_id IS NOT NULL) bd
LEFT JOIN(
SELECT date, type_day, hour, country, caller_id
FROM(
SELECT bd.date, type_day, bd.hour, bd.caller_id, country
FROM(
SELECT bd.date,bd.type_day,bd.hour,bd.caller_id,bd.created_at, bd.country
FROM base_data bd
LEFT JOIN(
SELECT date, type_day, hour, initial_country AS country, caller_id
FROM(
SELECT bd.date,bd.type_day,bd.hour,bd.caller_id,bd.created_at, bd.country AS initial_country
FROM base_data bd
LEFT JOIN base_data next_hour
ON bd.caller_id = next_hour.caller_id
AND TIMESTAMP_DIFF(TIMESTAMP(next_hour.created_at), TIMESTAMP(bd.created_at), SECOND) BETWEEN 1 AND 3600
AND next_hour.duration = 0
LEFT JOIN project_id.mysql.user_info u ON u.id=bd.caller_id AND DATE(created_at)=bd.date
WHERE bd.duration = 0 AND next_hour.caller_id IS NOT NULL AND u.id IS NOT NULL
GROUP BY all
UNION DISTINCT
SELECT bd.date,bd.type_day,bd.hour,bd.caller_id,bd.created_at, bd.country AS initial_country
FROM base_data bd
LEFT JOIN base_data next_hour
ON bd.caller_id = next_hour.caller_id
AND TIMESTAMP_DIFF(TIMESTAMP(next_hour.created_at), TIMESTAMP(bd.created_at), SECOND) BETWEEN 1 AND 3600
AND next_hour.caller_id IS NULL
LEFT JOIN project_id.mysql.user_info u ON u.id=bd.caller_id AND DATE(created_at)=bd.date
WHERE bd.duration = 0 AND next_hour.caller_id IS NULL AND u.id IS NOT NULL
GROUP BY all)
GROUP BY all) new_zero_activity_callers ON new_zero_activity_callers.date=bd.date AND new_zero_activity_callers.hour=bd.hour AND new_zero_activity_callers.caller_id=bd.caller_id AND new_zero_activity_callers.country=bd.country
WHERE bd.duration=0 AND new_zero_activity_callers.caller_id IS NOT NULL) bd
LEFT JOIN(
SELECT DATE(event_time) AS date, EXTRACT(HOUR FROM event_time) AS hour, EXTRACT(MINUTE FROM event_time) AS minute,caller_id
FROM project_id.provider_lack.provider_lack_event_view
WHERE DATE(event_time) BETWEEN '2025-01-01' AND CURRENT_DATE()-1
GROUP BY all
ORDER BY date DESC, hour ASC
) event ON event.caller_id=bd.caller_id AND event.date=bd.date AND event.hour=bd.hour AND event.minute=EXTRACT(MINUTE FROM bd.created_at)
WHERE event.caller_id IS NOT NULL
GROUP BY all)
GROUP BY all
UNION DISTINCT
SELECT date, type_day, hour, initial_country AS country, caller_id
FROM(
SELECT bd.date, bd.type_day, bd.hour, bd.caller_id, bd.created_at, bd.country AS initial_country
FROM base_data bd
LEFT JOIN project_id.mysql.user_info u ON u.id=bd.caller_id AND DATE(created_at)=bd.date
LEFT JOIN (
SELECT date, caller_id
FROM(
SELECT date, caller_id, SUM(duration) AS duration
FROM base_data
GROUP BY all)
WHERE duration=0
) caller_0 ON caller_0.caller_id=bd.caller_id AND caller_0.date=bd.date
WHERE bd.duration = 0 AND u.id IS NOT NULL AND caller_0.caller_id IS NOT NULL
GROUP BY all)
GROUP BY ALL
) zero_exp ON zero_exp.date=bd.date AND zero_exp.hour=bd.hour AND zero_exp.caller_id=bd.caller_id AND zero_exp.country=bd.country
WHERE zero_exp.caller_id IS NULL)
GROUP BY ALL
ORDER BY 1,2)
----providers lack (hourly)
SELECT bd.date, type_day, hour, bd.country, providers, callers_more_duration_0, new_callers, COALESCE(zero_activity_callers,0) AS zero_activity_callers, COALESCE(new_zero_activity_callers_60,0) AS new_zero_activity_callers_60, new_zero_experience_callers, CASE WHEN new_zero_final_callers!=(new_zero_activity_callers_60-new_zero_experience_callers) THEN (new_zero_activity_callers_60-new_zero_experience_callers) ELSE (new_zero_activity_callers_60-new_zero_experience_callers) END AS new_zero_final_callers, zero_providers, ROUND((new_zero_activity_callers_60-new_zero_experience_callers)/real_covered_callers) AS provider_lack_optimal, 'UTC' AS local_hour
FROM(
SELECT bd.date, bd.type_day, bd.hour, bd.country, bd.providers, bd.callers_0 AS callers_more_duration_0, bd.new_callers, zero_activity_callers,new_zero_activity_callers_60, COALESCE(new_zero_experience_callers,0) AS new_zero_experience_callers, COALESCE(new_zero_final_callers,0) AS new_zero_final_callers, COALESCE(zero_providers,0) AS zero_providers, ROUND(bd.callers_0/bd.providers,1) AS covered_callers
FROM basic_data bd
LEFT JOIN zero_activity_callers zp ON zp.date=bd.date AND zp.type_day=bd.type_day AND zp.hour=bd.hour AND zp.country=bd.country
LEFT JOIN zero_experience_callers uf ON uf.date=bd.date AND uf.type_day=bd.type_day AND uf.hour=bd.hour AND uf.country=bd.country
LEFT JOIN zero_providers zh ON zh.date=bd.date AND zh.type_day=bd.type_day AND zh.hour=bd.hour AND zh.country=bd.country
WHERE bd.country !='RegionF' ) bd
LEFT JOIN (
SELECT date, country, ROUND(APPROX_QUANTILES(covered_callers,100)[OFFSET(90)],2) AS real_covered_callers
FROM(
SELECT bd1.date, bd1.type_day, bd1.hour,bd1.country, bd2.date AS rolling_date, bd2.callers_0 / bd2.providers AS covered_callers
FROM basic_data bd1
LEFT JOIN basic_data bd2
ON bd2.date BETWEEN bd1.date - 36 AND bd1.date - 9
AND bd1.type_day = bd2.type_day
AND bd1.hour = bd2.hour
AND bd1.country = bd2.country
ORDER BY bd1.date, bd1.hour, bd1.country, rolling_date)
WHERE date>='2025-02-01'
GROUP BY date,country
ORDER BY 1,2
) real ON real.country=bd.country AND real.date=bd.date
WHERE bd.date>='2025-02-01'
ORDER BY date DESC, hour ASC;
----all geos
INSERT INTO project_id.provider_lack.provider_lack_hour
WITH basic_data AS (SELECT providers.date, providers.type_day, providers.hour,providers.country, providers.providers, callers_0.callers_0, new_callers
FROM(
SELECT date, type_day, hour, 'All' AS country, COUNT(DISTINCT provider_id) AS providers
FROM project_id.provider_lack.base_data
WHERE duration>0 AND DATE(date) BETWEEN '2025-01-01' AND CURRENT_DATE()-1
GROUP BY ALL) providers
--callers duration more than 0 seconds
LEFT JOIN(
SELECT date, type_day, hour, 'All' AS country, COUNT(DISTINCT caller_id) AS callers_0, COUNT(DISTINCT CASE WHEN DATE(created_at)=DATE(created_at) THEN caller_id END) AS new_callers
FROM project_id.provider_lack.base_data vp
LEFT JOIN project_id.mysql.user_info u ON u.id=vp.caller_id
WHERE duration>0 AND DATE(date) BETWEEN '2025-01-01' AND CURRENT_DATE()-1
GROUP BY all
ORDER BY 1 DESC, 3 ASC
) callers_0 ON providers.date=callers_0.date AND providers.type_day=callers_0.type_day AND providers.hour=callers_0.hour AND providers.country=callers_0.country),
---active providers (duration more than 0 second in hour)
active_callers AS (
SELECT date, type_day, hour, 'All' AS country, provider_id, duration
FROM project_id.provider_lack.base_data
WHERE DATE(date) BETWEEN '2025-01-01' AND CURRENT_DATE()-1
GROUP BY ALL
HAVING duration>0
),
----callers with more than 2 tries
more_than_2 AS(
SELECT date, type_day, hour, caller_id
FROM(
SELECT vp.date, vp.type_day, vp.hour,
caller_id, created_at
FROM project_id.provider_lack.base_data vp
LEFT JOIN active_callers au ON au.provider_id = vp.provider_id
AND au.date = vp.date
AND au.type_day = vp.type_day
AND au.hour=vp.hour
WHERE au.provider_id IS NOT NULL AND DATE(vp.date) BETWEEN '2025-01-01' AND CURRENT_DATE()-1
GROUP BY ALL)
GROUP BY ALL
HAVING COUNT(caller_id)>=2
),
----base data for further actions
base_data AS (
SELECT
vp.date,vp.type_day, vp.hour, 'All' AS country,
vp.caller_id,
vp.duration, vp.created_at, vp.provider_id
FROM project_id.provider_lack.base_data vp
LEFT JOIN active_callers au ON au.provider_id = vp.provider_id
AND au.date = vp.date
AND au.type_day = vp.type_day
AND au.hour=vp.hour
LEFT JOIN more_than_2 m2 ON m2.caller_id = vp.caller_id
AND m2.date = vp.date
AND m2.type_day = vp.type_day
AND m2.hour=vp.hour
WHERE au.provider_id IS NOT NULL
AND m2.caller_id IS NOT NULL
AND DATE(vp.date) BETWEEN '2025-01-01' AND CURRENT_DATE()-1
GROUP BY ALL
),
----zero activity callers and zero_activity_callers_60
zero_activity_callers AS(
SELECT zero_callers.date, zero_callers.type_day, zero_callers.hour, zero_callers.country, zero_activity_callers, COALESCE(new_zero_activity_callers_60,0) AS new_zero_activity_callers_60
FROM(
SELECT date, type_day, hour, country, COUNT(DISTINCT caller_id) AS zero_activity_callers
FROM(
SELECT bd.date,bd.type_day,bd.hour,bd.country,bd.caller_id
FROM base_data bd
WHERE bd.duration=0)
GROUP BY all) zero_callers
LEFT JOIN (
SELECT date, type_day, hour, initial_country AS country, COUNT(DISTINCT caller_id) AS new_zero_activity_callers_60
FROM(
SELECT bd.date,bd.type_day, bd.hour, bd.caller_id,bd.created_at, bd.country AS initial_country
FROM base_data bd
LEFT JOIN base_data next_hour
ON bd.caller_id = next_hour.caller_id
AND TIMESTAMP_DIFF(TIMESTAMP(next_hour.created_at), TIMESTAMP(bd.created_at), SECOND) BETWEEN 1 AND 3600
AND next_hour.duration = 0
LEFT JOIN project_id.mysql.user_info u ON u.id=bd.caller_id AND DATE(created_at)=bd.date
WHERE bd.duration = 0 AND next_hour.caller_id IS NOT NULL AND u.id IS NOT NULL
GROUP BY all
UNION DISTINCT
SELECT bd.date, bd.type_day, bd.hour, bd.caller_id, bd.created_at, bd.country AS initial_country
FROM base_data bd
LEFT JOIN base_data next_hour
ON bd.caller_id = next_hour.caller_id
AND TIMESTAMP_DIFF(TIMESTAMP(next_hour.created_at), TIMESTAMP(bd.created_at), SECOND) BETWEEN 1 AND 3600
AND next_hour.caller_id IS NULL
LEFT JOIN project_id.mysql.user_info u ON u.id=bd.caller_id AND DATE(created_at)=bd.date
WHERE bd.duration = 0 AND next_hour.caller_id IS NULL AND u.id IS NOT NULL
GROUP BY all)
GROUP BY all
) new_zero_60 ON new_zero_60.date=zero_callers.date AND new_zero_60.hour=zero_callers.hour AND new_zero_60.country=zero_callers.country
ORDER BY date DESC, hour ASC),
--zero_experience_callers
zero_experience_callers AS (
SELECT date, type_day, hour, country, COUNT(DISTINCT caller_id) AS new_zero_experience_callers
FROM(
SELECT date, type_day, hour, country, caller_id
FROM(
SELECT bd.date, type_day, bd.hour, bd.caller_id, country
FROM(
SELECT bd.date, bd.type_day, bd.hour, bd.caller_id, bd.created_at, bd.country,
FROM base_data bd
LEFT JOIN(
SELECT date, type_day, hour, initial_country AS country, caller_id
FROM(
SELECT bd.date, bd.type_day, bd.hour,bd.caller_id, bd.created_at, bd.country AS initial_country
FROM base_data bd
LEFT JOIN base_data next_hour
ON bd.caller_id = next_hour.caller_id
AND TIMESTAMP_DIFF(TIMESTAMP(next_hour.created_at), TIMESTAMP(bd.created_at), SECOND) BETWEEN 1 AND 3600
AND next_hour.duration = 0
LEFT JOIN project_id.mysql.user_info u ON u.id=bd.caller_id AND DATE(created_at)=bd.date
WHERE bd.duration = 0 AND next_hour.caller_id IS NOT NULL AND u.id IS NOT NULL
GROUP BY all
UNION DISTINCT
SELECT bd.date, bd.type_day, bd.hour, bd.caller_id, bd.created_at, bd.country AS initial_country
FROM base_data bd
LEFT JOIN base_data next_hour
ON bd.caller_id = next_hour.caller_id
AND TIMESTAMP_DIFF(TIMESTAMP(next_hour.created_at), TIMESTAMP(bd.created_at), SECOND) BETWEEN 1 AND 3600
AND next_hour.caller_id IS NULL
LEFT JOIN project_id.mysql.user_info u ON u.id=bd.caller_id AND DATE(created_at)=bd.date
WHERE bd.duration = 0 AND next_hour.caller_id IS NULL AND u.id IS NOT NULL
GROUP BY all)
GROUP BY all) new_zero_activity_callers ON new_zero_activity_callers.date=bd.date AND new_zero_activity_callers.hour=bd.hour AND new_zero_activity_callers.caller_id=bd.caller_id AND new_zero_activity_callers.country=bd.country
WHERE bd.duration=0 AND new_zero_activity_callers.caller_id IS NOT NULL) bd
LEFT JOIN(
SELECT DATE(event_time) AS date, EXTRACT(HOUR FROM event_time) AS hour, EXTRACT(MINUTE FROM event_time) AS minute,caller_id
FROM project_id.provider_lack.provider_lack_event_view
WHERE DATE(event_time) BETWEEN '2025-01-01' AND CURRENT_DATE()-1
GROUP BY all
ORDER BY date, hour ASC
) event ON event.caller_id=bd.caller_id AND event.date=bd.date AND event.hour=bd.hour AND event.minute=EXTRACT(MINUTE FROM bd.created_at)
WHERE event.caller_id IS NOT NULL
GROUP BY all)
GROUP BY all
UNION DISTINCT
SELECT date, type_day, hour, country AS country, caller_id
FROM(
SELECT bd.date,bd.type_day,bd.hour, bd.caller_id, bd.created_at,bd.country
FROM base_data bd
LEFT JOIN project_id.mysql.user_info u ON u.id=bd.caller_id AND DATE(created_at)=bd.date
LEFT JOIN (
SELECT date, caller_id
FROM(
SELECT date, caller_id, SUM(duration) AS duration
FROM base_data
GROUP BY all)
WHERE duration=0
) caller_0 ON caller_0.caller_id=bd.caller_id AND caller_0.date=bd.date
WHERE bd.duration = 0 AND u.id IS NOT NULL AND caller_0.caller_id IS NOT NULL
GROUP BY all)
GROUP BY all)
GROUP BY all
ORDER BY date DESC, hour ASC),
------zero providers for correlation
zero_providers AS (SELECT date, type_day, hour, country, COUNT(DISTINCT caller_id) AS new_zero_final_callers, COUNT(DISTINCT provider_id) AS zero_providers
FROM(
SELECT bd.date, bd.type_day, bd.hour, bd.country, bd.caller_id, bd.provider_id
FROM(
SELECT bd.date,bd.type_day,bd.hour, bd.caller_id,
bd.created_at,
bd.country,
bd.provider_id
FROM base_data bd
LEFT JOIN(
SELECT date, type_day, hour, initial_country AS country, caller_id
FROM(
SELECT
bd.date,
bd.type_day,
bd.hour,
bd.caller_id,
bd.created_at,
bd.country AS initial_country,
FROM base_data bd
LEFT JOIN base_data next_hour
ON bd.caller_id = next_hour.caller_id
AND TIMESTAMP_DIFF(TIMESTAMP(next_hour.created_at), TIMESTAMP(bd.created_at), SECOND) BETWEEN 1 AND 3600
AND next_hour.duration = 0
LEFT JOIN project_id.mysql.user_info u ON u.id=bd.caller_id AND DATE(created_at)=bd.date
WHERE bd.duration = 0 AND next_hour.caller_id IS NOT NULL AND u.id IS NOT NULL
GROUP BY all
UNION DISTINCT
SELECT
bd.date,
bd.type_day,
bd.hour,
bd.caller_id,
bd.created_at,
bd.country AS initial_country,
FROM base_data bd
LEFT JOIN base_data next_hour
ON bd.caller_id = next_hour.caller_id
AND TIMESTAMP_DIFF(TIMESTAMP(next_hour.created_at), TIMESTAMP(bd.created_at), SECOND) BETWEEN 1 AND 3600
AND next_hour.caller_id IS NULL
LEFT JOIN project_id.mysql.user_info u ON u.id=bd.caller_id AND DATE(created_at)=bd.date
WHERE bd.duration = 0 AND next_hour.caller_id IS NULL AND u.id IS NOT NULL
GROUP BY all)
GROUP BY all) new_zero_activity_callers ON new_zero_activity_callers.date=bd.date AND new_zero_activity_callers.hour=bd.hour AND new_zero_activity_callers.caller_id=bd.caller_id AND new_zero_activity_callers.country=bd.country
WHERE bd.duration=0 AND new_zero_activity_callers.caller_id IS NOT NULL) bd
LEFT JOIN(
SELECT date, type_day, hour, country, caller_id
FROM(
SELECT bd.date, type_day, bd.hour, bd.caller_id, country
FROM(
SELECT
bd.date,
bd.type_day,
bd.hour,
bd.caller_id,
bd.created_at,
bd.country,
FROM base_data bd
LEFT JOIN(
SELECT date, type_day, hour, initial_country AS country, caller_id
FROM(
SELECT
bd.date,
bd.type_day,
bd.hour,
bd.caller_id,
bd.created_at,
bd.country AS initial_country,
FROM base_data bd
LEFT JOIN base_data next_hour
ON bd.caller_id = next_hour.caller_id
AND TIMESTAMP_DIFF(TIMESTAMP(next_hour.created_at), TIMESTAMP(bd.created_at), SECOND) BETWEEN 1 AND 3600
AND next_hour.duration = 0
LEFT JOIN project_id.mysql.user_info u ON u.id=bd.caller_id AND DATE(created_at)=bd.date
WHERE bd.duration = 0 AND next_hour.caller_id IS NOT NULL AND u.id IS NOT NULL
GROUP BY all
UNION DISTINCT
SELECT
bd.date,
bd.type_day,
bd.hour,
bd.caller_id,
bd.created_at,
bd.country AS initial_country,
FROM base_data bd
LEFT JOIN base_data next_hour
ON bd.caller_id = next_hour.caller_id
AND TIMESTAMP_DIFF(TIMESTAMP(next_hour.created_at), TIMESTAMP(bd.created_at), SECOND) BETWEEN 1 AND 3600
AND next_hour.caller_id IS NULL
LEFT JOIN project_id.mysql.user_info u ON u.id=bd.caller_id AND DATE(created_at)=bd.date
WHERE bd.duration = 0 AND next_hour.caller_id IS NULL AND u.id IS NOT NULL
GROUP BY all)
GROUP BY all) new_zero_activity_callers ON new_zero_activity_callers.date=bd.date AND new_zero_activity_callers.hour=bd.hour AND new_zero_activity_callers.caller_id=bd.caller_id AND new_zero_activity_callers.country=bd.country
WHERE bd.duration=0 AND new_zero_activity_callers.caller_id IS NOT NULL) bd
LEFT JOIN(
SELECT DATE(event_time) AS date, EXTRACT(HOUR FROM event_time) AS hour, EXTRACT(MINUTE FROM event_time) AS minute,caller_id
FROM project_id.provider_lack.provider_lack_event_view
WHERE DATE(event_time) BETWEEN '2025-01-01' AND CURRENT_DATE()-1
GROUP BY all
ORDER BY date DESC, hour ASC
) event ON event.caller_id=bd.caller_id AND event.date=bd.date AND event.hour=bd.hour AND event.minute=EXTRACT(MINUTE FROM bd.created_at)
WHERE event.caller_id IS NOT NULL
GROUP BY all)
GROUP BY all
UNION DISTINCT
SELECT date, type_day, hour, initial_country AS country, caller_id
FROM(
SELECT
bd.date,
bd.type_day,
bd.hour,
bd.caller_id,
bd.created_at,
bd.country AS initial_country
FROM base_data bd
LEFT JOIN project_id.mysql.user_info u ON u.id=bd.caller_id AND DATE(created_at)=bd.date
LEFT JOIN (
SELECT date, caller_id
FROM(
SELECT date, caller_id, SUM(duration) AS duration
FROM base_data
GROUP BY all)
WHERE duration=0
) caller_0 ON caller_0.caller_id=bd.caller_id AND caller_0.date=bd.date
WHERE bd.duration = 0 AND u.id IS NOT NULL AND caller_0.caller_id IS NOT NULL
GROUP BY all)
GROUP BY ALL
) zero_exp ON zero_exp.date=bd.date AND zero_exp.hour=bd.hour AND zero_exp.caller_id=bd.caller_id AND zero_exp.country=bd.country
WHERE zero_exp.caller_id IS NULL)
GROUP BY ALL
ORDER BY 1,2)
SELECT bd.date, type_day, bd.hour, bd.country, providers, callers_more_duration_0, new_callers, zero_activity_callers, new_zero_activity_callers_60, new_zero_experience_callers, CASE WHEN new_zero_final_callers!=(new_zero_activity_callers_60-new_zero_experience_callers) THEN (new_zero_activity_callers_60-new_zero_experience_callers) ELSE (new_zero_activity_callers_60-new_zero_experience_callers) END AS new_zero_final_callers, zero_providers, ROUND((new_zero_activity_callers_60-new_zero_experience_callers)/real_covered_callers) AS provider_lack_optimal, 'UTC' AS local_hour
FROM(
SELECT bd.date, bd.type_day, bd.hour, bd.country, bd.providers, bd.callers_0 AS callers_more_duration_0, bd.new_callers, zero_activity_callers,new_zero_activity_callers_60, COALESCE(new_zero_experience_callers,0) AS new_zero_experience_callers, COALESCE(new_zero_final_callers,0) AS new_zero_final_callers, COALESCE(zero_providers,0) AS zero_providers, ROUND(bd.callers_0/bd.providers,1) AS covered_callers
FROM basic_data bd
LEFT JOIN zero_activity_callers zp ON zp.date=bd.date AND zp.type_day=bd.type_day AND zp.hour=bd.hour AND zp.country=bd.country
LEFT JOIN zero_experience_callers uf ON uf.date=bd.date AND uf.type_day=bd.type_day AND uf.hour=bd.hour AND uf.country=bd.country
LEFT JOIN zero_providers zh ON zh.date=bd.date AND zh.type_day=bd.type_day AND zh.hour=bd.hour AND zh.country=bd.country
WHERE bd.country !='RegionF' ) bd
LEFT JOIN (
SELECT date, country, ROUND(APPROX_QUANTILES(covered_callers,100)[OFFSET(90)],2) AS real_covered_callers
FROM(
SELECT bd1.date, bd1.type_day, bd1.hour,bd1.country, bd2.date AS rolling_date, bd2.callers_0 / bd2.providers AS covered_callers
FROM basic_data bd1
LEFT JOIN basic_data bd2
ON bd2.date BETWEEN bd1.date - 36 AND bd1.date - 9
AND bd1.type_day = bd2.type_day
AND bd1.hour = bd2.hour
AND bd1.country = bd2.country
ORDER BY bd1.date, bd1.hour, bd1.country, rolling_date)
WHERE date>='2025-02-01'
GROUP BY date,country
ORDER BY 1,2
) real ON real.country=bd.country AND real.date=bd.date
WHERE bd.date>='2025-02-01'
ORDER BY date DESC, hour ASC;
---------------------local hours
INSERT INTO project_id.provider_lack.provider_lack_hour
--providers lack
----providers lack (local hour)
SELECT DATE(local_datetime) AS date,CASE WHEN EXTRACT(HOUR FROM local_datetime) between 0 and 5 then 'night'
WHEN EXTRACT(HOUR FROM local_datetime) between 6 and 11 then 'morning'
WHEN EXTRACT(HOUR FROM local_datetime) between 12 and 17 then 'day'
WHEN EXTRACT(HOUR FROM local_datetime) between 18 and 23 then 'evening' end as type_day , EXTRACT(hour FROM local_datetime) AS hour, country, providers, callers_more_duration_0, new_callers, zero_activity_callers, new_zero_activity_callers_60, new_zero_experience_callers, new_zero_final_callers, zero_providers, provider_lack_optimal, 'Local Hour' AS local_hour
FROM(
SELECT bd.date, bd.type_day, bd.hour, bd.country, bd.providers, callers_more_duration_0, bd.new_callers, zero_activity_callers,new_zero_activity_callers_60, COALESCE(new_zero_experience_callers,0) AS new_zero_experience_callers, COALESCE(new_zero_final_callers,0) AS new_zero_final_callers, COALESCE(zero_providers,0) AS zero_providers, ROUND(bd.callers_more_duration_0/bd.providers,1) AS covered_callers,
CASE WHEN bd.country = 'RegionA' THEN TIMESTAMP(DATETIME(bd.date, TIME(bd.hour, 0, 0)),"RegionA/Timezone")
WHEN bd.country = 'RegionB' THEN TIMESTAMP(DATETIME(bd.date, TIME(bd.hour, 0, 0)),"RegionB/Timezone")
WHEN bd.country = 'RegionC' THEN TIMESTAMP(DATETIME(bd.date, TIME(bd.hour, 0, 0)),"RegionC/Timezone")
WHEN bd.country = 'RegionD' THEN TIMESTAMP(DATETIME(bd.date, TIME(bd.hour, 0, 0)),"RegionD/Timezone")
WHEN bd.country = 'RegionE' THEN TIMESTAMP(DATETIME(bd.date, TIME(bd.hour, 0, 0)),"RegionE/Timezone")
ELSE TIMESTAMP(DATETIME(bd.date, TIME(bd.hour, 0, 0))) END AS local_datetime, provider_lack_optimal
FROM project_id.provider_lack.provider_lack_hour bd
WHERE bd.country NOT IN('RegionF','All') )
ORDER BY date DESC, hour ASC;
--------------anomalies detecting
CREATE OR REPLACE TABLE project_id.provider_lack.provider_lack_hour AS
WITH stats AS (
SELECT
country, local_hour,
date, hour,
AVG(providers) OVER(PARTITION BY country, local_hour ORDER BY date, CAST(FORMAT_DATE('%Y%m%d', date) AS INT64) * 10+ hour
ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING) AS avg_providers,
STDDEV(providers) OVER(PARTITION BY country, local_hour ORDER BY date, CAST(FORMAT_DATE('%Y%m%d', date) AS INT64) * 10+ hour
ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS stddev_providers,
AVG(callers_more_duration_0) OVER(PARTITION BY country, local_hour ORDER BY date, CAST(FORMAT_DATE('%Y%m%d', date) AS INT64) * 10+ hour
ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING) AS avg_callers,
STDDEV(callers_more_duration_0) OVER(PARTITION BY country, local_hour ORDER BY date, CAST(FORMAT_DATE('%Y%m%d', date) AS INT64) * 10+ hour
ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS stddev_callers,
AVG(new_callers) OVER(PARTITION BY country, local_hour ORDER BY date, CAST(FORMAT_DATE('%Y%m%d', date) AS INT64) * 10+ hour
ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING) AS avg_new_callers,
STDDEV(new_callers) OVER(PARTITION BY country, local_hour ORDER BY date, CAST(FORMAT_DATE('%Y%m%d', date) AS INT64) * 10+ hour
ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS stddev_new_callers,
AVG(zero_activity_callers) OVER(PARTITION BY country, local_hour ORDER BY date, CAST(FORMAT_DATE('%Y%m%d', date) AS INT64) * 10+ hour
ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING) AS avg_zero_activity_callers,
STDDEV(zero_activity_callers) OVER(PARTITION BY country, local_hour ORDER BY date, CAST(FORMAT_DATE('%Y%m%d', date) AS INT64) * 10+ hour
ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS stddev_zero_activity_callers,
AVG(new_zero_activity_callers_60) OVER(PARTITION BY country, local_hour ORDER BY date, CAST(FORMAT_DATE('%Y%m%d', date) AS INT64) * 10+ hour
ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING) AS avg_new_zero_activity_callers_60,
STDDEV(new_zero_activity_callers_60) OVER(PARTITION BY country, local_hour ORDER BY date, CAST(FORMAT_DATE('%Y%m%d', date) AS INT64) * 10+ hour
ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS stddev_new_zero_activity_callers_60,
AVG(new_zero_experience_callers) OVER(PARTITION BY country, local_hour ORDER BY date, CAST(FORMAT_DATE('%Y%m%d', date) AS INT64) * 10+ hour
  ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING) AS avg_new_zero_exp_callers,
STDDEV(new_zero_experience_callers) OVER(PARTITION BY country, local_hour ORDER BY date, CAST(FORMAT_DATE('%Y%m%d', date) AS INT64) * 10+ hour ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS stddev_new_zero_exp_callers,
AVG(new_zero_final_callers) OVER(PARTITION BY country, local_hour ORDER BY date, CAST(FORMAT_DATE('%Y%m%d', date) AS INT64) * 10+ hour
ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING) AS avg_new_zero_final_callers,
STDDEV(new_zero_final_callers) OVER(PARTITION BY country, local_hour ORDER BY date, CAST(FORMAT_DATE('%Y%m%d', date) AS INT64) * 10+ hour
ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS stddev_new_zero_final_callers,
AVG(zero_providers) OVER(PARTITION BY country, local_hour ORDER BY date, CAST(FORMAT_DATE('%Y%m%d', date) AS INT64) * 10+ hour
ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING) AS avg_zero_providers,
STDDEV(zero_providers) OVER(PARTITION BY country, local_hour ORDER BY date, CAST(FORMAT_DATE('%Y%m%d', date) AS INT64) * 10+ hour
ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS stddev_zero_providers,
AVG(provider_lack_optimal) OVER(PARTITION BY country, local_hour ORDER BY date, CAST(FORMAT_DATE('%Y%m%d', date) AS INT64) * 10+ hour
ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING) AS avg_provider_lack_optimal,
STDDEV(provider_lack_optimal) OVER(PARTITION BY country, local_hour ORDER BY date, CAST(FORMAT_DATE('%Y%m%d', date) AS INT64) * 10+ hour
ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS stddev_provider_lack_optimal
FROM
project_id.provider_lack.provider_lack_hour
),
-- Step 2: Replace anomalies in the data
anomalies_replaced AS (
SELECT
h.date,
type_day,
h.hour,
h.country,
CASE
WHEN ABS(providers - stats.avg_providers) > 2 * stats.stddev_providers THEN CAST(stats.avg_providers AS INT64)
ELSE providers
END AS providers,
CASE
WHEN ABS(callers_more_duration_0 - stats.avg_callers) > 2 * stats.stddev_callers THEN CAST(stats.avg_callers AS INT64)
ELSE callers_more_duration_0
END AS callers_more_duration_0,
CASE
WHEN ABS(new_callers - stats.avg_new_callers) > 2 * stats.stddev_new_callers THEN CAST(stats.avg_new_callers AS INT64)
ELSE new_callers
END AS new_callers,
CASE
WHEN ABS(zero_activity_callers - stats.avg_zero_activity_callers) > 2 * stats.stddev_zero_activity_callers THEN CAST(stats.avg_zero_activity_callers AS INT64)
ELSE zero_activity_callers
END AS zero_activity_callers,
CASE
WHEN ABS(new_zero_activity_callers_60 - stats.avg_new_zero_activity_callers_60) > 2 * stats.stddev_new_zero_activity_callers_60 THEN CAST(stats.avg_new_zero_activity_callers_60 AS INT64)
ELSE new_zero_activity_callers_60
END AS new_zero_activity_callers_60,
CASE
WHEN ABS(new_zero_experience_callers - stats.avg_new_zero_exp_callers) > 2 * stats.stddev_new_zero_exp_callers THEN CAST(stats.avg_new_zero_exp_callers AS INT64)
ELSE new_zero_experience_callers
END AS new_zero_experience_callers,
CASE
WHEN ABS(new_zero_final_callers - stats.avg_new_zero_final_callers) > 2 * stats.stddev_new_zero_final_callers THEN CAST(stats.avg_new_zero_final_callers AS INT64)
ELSE new_zero_final_callers
END AS new_zero_final_callers,
CASE
WHEN ABS(zero_providers - stats.avg_zero_providers) > 2 * stats.stddev_zero_providers THEN CAST(stats.avg_zero_providers AS INT64)
ELSE zero_providers
END AS zero_providers,
CASE
WHEN ABS(provider_lack_optimal - stats.avg_provider_lack_optimal) > 2 * stats.stddev_provider_lack_optimal THEN CAST(stats.avg_provider_lack_optimal AS INT64)
ELSE provider_lack_optimal
END AS provider_lack_optimal,
h.local_hour
FROM
project_id.provider_lack.provider_lack_hour h
JOIN
stats
ON
h.country = stats.country
AND h.date = stats.date
AND h.hour=stats.hour
AND h.local_hour=stats.local_hour
)
SELECT * FROM anomalies_replaced
ORDER BY date, hour;
------------provider lack hour processed
TRUNCATE TABLE project_id.provider_lack.provider_lack_hour_processed;
INSERT INTO project_id.provider_lack.provider_lack_hour_processed
SELECT *, ROUND(provider_lack_optimal*perc_of_zero_callers_on_providers,0) AS provider_lack_from_perc, ROUND((provider_lack_optimal*perc_of_zero_callers_on_providers)/(providers/prev_provider_value),0) AS provider_lack_final
FROM(
SELECT *, LAG(providers) OVER(PARTITION BY country, local_hour ORDER BY date ASC,CAST(FORMAT_DATE('%Y%m%d', date) AS INT64) * 10+hour) AS prev_provider_value, ROUND(new_zero_final_callers/providers,2) AS perc_of_zero_callers_on_providers
FROM project_id.provider_lack.provider_lack_hour)
WHERE prev_provider_value IS NOT NULL AND date>='2025-03-01'
ORDER BY date;
------------------------------- all the information for hour (comparison provider, callers, provider lack)
TRUNCATE TABLE project_id.provider_lack.lack_hour;
INSERT INTO project_id.provider_lack.lack_hour
SELECT datetime, CONCAT(DATE(datetime),' ',day_hour) AS day_hour, sorting, hour.country, average_providers, CAST(providers AS INT64) AS providers, average_callers, CAST(callers_more_duration_0 AS INT64) AS callers_more_duration_0, CAST(new_callers AS INT64) AS new_callers, average_new_callers,average_provider_lack, provider_lack_final, CAST(new_zero_final_callers AS INT64) AS new_zero_final_callers, average_new_zero_final_callers, CAST(zero_providers AS INT64) AS zero_providers, average_zero_providers, average_provider_lack/average_providers AS percent_of_providers_lack, percent_of_providers_fact, q1,q2,q3, FORMAT_DATETIME('%A',datetime) AS weekday, 'UTC' AS local_hour
FROM(
SELECT CONCAT(FORMAT_DATE('%A', date), ' ', CAST(hour AS STRING)) AS day_hour, country,
CAST(FORMAT_DATE('%Y%m%d', date) AS INT64) * 100+
CASE WHEN FORMAT_DATE('%A',date)='Monday' THEN 0
WHEN FORMAT_DATE('%A',date)='Tuesday' THEN 1
WHEN FORMAT_DATE('%A',date)='Wednesday' THEN 2
WHEN FORMAT_DATE('%A',date)='Thursday' THEN 3
WHEN FORMAT_DATE('%A',date)='Friday' THEN 4
WHEN FORMAT_DATE('%A',date)='Saturday' THEN 5
WHEN FORMAT_DATE('%A',date)='Sunday' THEN 6 END +hour AS sorting, ROUND(AVG(providers) OVER (PARTITION BY FORMAT_DATE('%A', date), hour, country ORDER BY date ASC )) AS average_providers, ROUND(AVG(callers_more_duration_0) OVER (PARTITION BY FORMAT_DATE('%A', date), hour, country ORDER BY date ASC )) AS average_callers,ROUND(AVG(new_callers) OVER (PARTITION BY FORMAT_DATE('%A', date), hour, country ORDER BY date ASC )) AS average_new_callers, ROUND(AVG(new_zero_final_callers) OVER (PARTITION BY FORMAT_DATE('%A', date), hour, country ORDER BY date ASC )) AS average_new_zero_final_callers, ROUND(AVG(zero_providers) OVER (PARTITION BY FORMAT_DATE('%A', date), hour, country ORDER BY date ASC )) AS average_zero_providers, ROUND(AVG(provider_lack_final) OVER (PARTITION BY FORMAT_DATE('%A', date), hour, country ORDER BY date ASC)) AS average_provider_lack
FROM project_id.provider_lack.provider_lack_hour_processed
WHERE date BETWEEN CURRENT_DATE()-36 AND CURRENT_DATE()-9 AND local_hour='UTC'
QUALIFY ROW_NUMBER() OVER (PARTITION BY day_hour, country ORDER BY date DESC) = 1) hour
LEFT JOIN(
SELECT country, CASE WHEN q1>0 THEN q1 ELSE avg_q1 END AS q1, CASE WHEN q2>0 THEN q2 ELSE avg_q2 END AS q2, CASE WHEN q3>0 THEN q3 ELSE avg_q3 END AS q3
FROM(
SELECT country, ROUND(APPROX_QUANTILES(percent_providers_lack,100)[OFFSET(65)],3) AS q1, ROUND(APPROX_QUANTILES(percent_providers_lack,100)[OFFSET(85)],3) AS q2, ROUND(APPROX_QUANTILES(percent_providers_lack,100)[OFFSET(95)],3) AS q3
FROM(
SELECT *, provider_lack_final/providers AS percent_providers_lack
FROM project_id.provider_lack.provider_lack_hour_processed
WHERE local_hour='UTC'
ORDER BY date DESC)
GROUP BY all)
CROSS JOIN (
SELECT AVG(q1) AS avg_q1, AVG(q2) AS avg_q2, AVG(q3) AS avg_q3
FROM(
SELECT country, ROUND(APPROX_QUANTILES(percent_providers_lack,100)[OFFSET(65)],3) AS q1, ROUND(APPROX_QUANTILES(percent_providers_lack,100)[OFFSET(85)],3) AS q2, ROUND(APPROX_QUANTILES(percent_providers_lack,100)[OFFSET(95)],3) AS q3
FROM(
SELECT *, provider_lack_final/providers AS percent_providers_lack
FROM project_id.provider_lack.provider_lack_hour_processed
WHERE local_hour='UTC'
ORDER BY date DESC)
GROUP BY all)
)
) quart ON quart.country=hour.country
INNER JOIN(
SELECT date,type_day,hour,host.country, providers,callers_more_duration_0, new_callers, new_zero_final_callers, zero_providers, provider_lack_final, DATETIME(CONCAT(CAST(date AS STRING), ' ', CAST(hour AS STRING), ':00:00')) AS datetime, provider_lack_final/providers AS percent_of_providers_fact
FROM project_id.provider_lack.provider_lack_hour_processed host
WHERE date BETWEEN CURRENT_DATE()-8 AND CURRENT_DATE()-2 AND local_hour='UTC'
) fact ON fact.country=hour.country AND CONCAT(FORMAT_DATE('%A', fact.date), ' ', CAST(fact.hour AS STRING))=day_hour
WHERE percent_of_providers_fact IS NOT NULL
UNION ALL
SELECT datetime, CONCAT(DATE(datetime),' ',day_hour) AS day_hour, sorting, hour.country, average_providers, CAST(providers AS INT64) AS providers, average_callers, CAST(callers_more_duration_0 AS INT64) AS callers_more_duration_0, CAST(new_callers AS INT64) AS new_callers, average_new_callers,average_provider_lack, provider_lack_final, CAST(new_zero_final_callers AS INT64) AS new_zero_final_callers, average_new_zero_final_callers, CAST(zero_providers AS INT64) AS zero_providers, average_zero_providers, average_provider_lack/average_providers AS percent_of_providers_lack, percent_of_providers_fact, q1,q2,q3, FORMAT_DATETIME('%A',datetime) AS weekday, 'Local Time' AS local_hour
FROM(
SELECT CONCAT(FORMAT_DATE('%A', date), ' ', CAST(hour AS STRING)) AS day_hour, country,
CAST(FORMAT_DATE('%Y%m%d', date) AS INT64) * 100+
CASE WHEN FORMAT_DATE('%A',date)='Monday' THEN 0
WHEN FORMAT_DATE('%A',date)='Tuesday' THEN 1
WHEN FORMAT_DATE('%A',date)='Wednesday' THEN 2
WHEN FORMAT_DATE('%A',date)='Thursday' THEN 3
WHEN FORMAT_DATE('%A',date)='Friday' THEN 4
WHEN FORMAT_DATE('%A',date)='Saturday' THEN 5
WHEN FORMAT_DATE('%A',date)='Sunday' THEN 6 END +hour AS sorting, ROUND(AVG(providers) OVER (PARTITION BY FORMAT_DATE('%A', date), hour, country ORDER BY date ASC )) AS average_providers, ROUND(AVG(callers_more_duration_0) OVER (PARTITION BY FORMAT_DATE('%A', date), hour, country ORDER BY date ASC )) AS average_callers, ROUND(AVG(new_callers) OVER (PARTITION BY FORMAT_DATE('%A', date), hour, country ORDER BY date ASC )) AS average_new_callers, ROUND(AVG(new_zero_final_callers) OVER (PARTITION BY FORMAT_DATE('%A', date), hour, country ORDER BY date ASC )) AS average_new_zero_final_callers, ROUND(AVG(zero_providers) OVER (PARTITION BY FORMAT_DATE('%A', date), hour, country ORDER BY date ASC )) AS average_zero_providers, ROUND(AVG(provider_lack_final) OVER (PARTITION BY FORMAT_DATE('%A', date), hour, country ORDER BY date ASC)) AS average_provider_lack
FROM project_id.provider_lack.provider_lack_hour_processed
WHERE CASE WHEN country='RegionA' THEN date BETWEEN CURRENT_DATE('RegionA/Timezone')-36 AND CURRENT_DATE('RegionA/Timezone')-9
WHEN country='RegionE' THEN date BETWEEN CURRENT_DATE('RegionE/Timezone')-36 AND CURRENT_DATE('RegionE/Timezone')-9
WHEN country='RegionC' THEN date BETWEEN CURRENT_DATE('RegionC/Timezone')-36 AND CURRENT_DATE('RegionC/Timezone')-9
WHEN country='RegionD' THEN date BETWEEN CURRENT_DATE('RegionD/Timezone')-36 AND CURRENT_DATE('RegionD/Timezone')-9
WHEN country='RegionB' THEN date BETWEEN CURRENT_DATE('RegionB/Timezone')-36 AND CURRENT_DATE('RegionB/Timezone')-9 END AND local_hour='Local Hour'
QUALIFY ROW_NUMBER() OVER (PARTITION BY day_hour, country ORDER BY date DESC) = 1) hour
LEFT JOIN(
SELECT country, CASE WHEN q1>0 THEN q1 ELSE avg_q1 END AS q1, CASE WHEN q2>0 THEN q2 ELSE avg_q2 END AS q2, CASE WHEN q3>0 THEN q3 ELSE avg_q3 END AS q3
FROM(
SELECT country, ROUND(APPROX_QUANTILES(percent_providers_lack,100)[OFFSET(65)],3) AS q1, ROUND(APPROX_QUANTILES(percent_providers_lack,100)[OFFSET(85)],3) AS q2, ROUND(APPROX_QUANTILES(percent_providers_lack,100)[OFFSET(95)],3) AS q3
FROM(
SELECT *, provider_lack_final/providers AS percent_providers_lack
FROM project_id.provider_lack.provider_lack_hour_processed
WHERE local_hour='Local Hour'
ORDER BY date DESC)
GROUP BY all)
CROSS JOIN (
SELECT AVG(q1) AS avg_q1, AVG(q2) AS avg_q2, AVG(q3) AS avg_q3
FROM(
SELECT country, ROUND(APPROX_QUANTILES(percent_providers_lack,100)[OFFSET(65)],3) AS q1, ROUND(APPROX_QUANTILES(percent_providers_lack,100)[OFFSET(85)],3) AS q2, ROUND(APPROX_QUANTILES(percent_providers_lack,100)[OFFSET(95)],3) AS q3
FROM(
SELECT *, provider_lack_final/providers AS percent_providers_lack
FROM project_id.provider_lack.provider_lack_hour_processed
WHERE local_hour='Local Hour'
ORDER BY date DESC)
GROUP BY all)
)
) quart ON quart.country=hour.country
INNER JOIN(
SELECT date,type_day,hour,host.country, providers,callers_more_duration_0, new_callers, new_zero_final_callers, zero_providers, provider_lack_final, DATETIME(CONCAT(CAST(date AS STRING), ' ', CAST(hour AS STRING), ':00:00')) AS datetime, provider_lack_final/providers AS percent_of_providers_fact
FROM project_id.provider_lack.provider_lack_hour_processed host
WHERE CASE WHEN country='RegionA' THEN date BETWEEN CURRENT_DATE('RegionA/Timezone')-8 AND CURRENT_DATE('RegionA/Timezone')-2
WHEN country='RegionE' THEN date BETWEEN CURRENT_DATE('RegionE/Timezone')-8 AND CURRENT_DATE('RegionE/Timezone')-2
WHEN country='RegionC' THEN date BETWEEN CURRENT_DATE('RegionC/Timezone')-8 AND CURRENT_DATE('RegionC/Timezone')-2
WHEN country='RegionD' THEN date BETWEEN CURRENT_DATE('RegionD/Timezone')-8 AND CURRENT_DATE('RegionD/Timezone')-2
WHEN country='RegionB' THEN date BETWEEN CURRENT_DATE('RegionB/Timezone')-8 AND CURRENT_DATE('RegionB/Timezone')-2 END AND local_hour='Local Hour'
) fact ON fact.country=hour.country AND CONCAT(FORMAT_DATE('%A', fact.date), ' ', CAST(fact.hour AS STRING))=day_hour
WHERE percent_of_providers_fact IS NOT NULL
ORDER BY datetime ASC;
------------------------------for the first plot of the section hour
TRUNCATE TABLE project_id.provider_lack.avg_hour;
INSERT INTO project_id.provider_lack.avg_hour
SELECT weekday, EXTRACT(HOUR FROM datetime) AS hour, country, average_providers, average_provider_lack, percent_of_providers_fact, percent_of_providers_lack, q1,q2,q3, local_hour
FROM project_id.provider_lack.lack_hour
WHERE DATE(datetime) BETWEEN CURRENT_DATE()-8 AND CURRENT_DATE()-2;
-------------------------------okr hour
TRUNCATE TABLE project_id.provider_lack.okr_hour;
INSERT INTO project_id.provider_lack.okr_hour
SELECT DATE_TRUNC(date, WEEK(MONDAY)) AS week_start, hour, AVG(provider_lack_final / providers) AS avg_percent_of_providers_lack
FROM `project_id.provider_lack.provider_lack_hour_processed`
WHERE local_hour = 'UTC'
AND country = 'All'
AND date BETWEEN '2025-03-01' AND DATE_SUB(DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)), INTERVAL 1 DAY)
GROUP BY week_start, hour
HAVING week_start>='2025-03-01'
ORDER BY week_start, hour;
