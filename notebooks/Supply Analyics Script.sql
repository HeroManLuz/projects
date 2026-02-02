DECLARE p_1 INT64;
DECLARE p_60 INT64;
DECLARE p_l_1 INT64;
DECLARE p_l_60 INT64;
DECLARE p_n_1 INT64;
DECLARE p_n_60 INT64;
SET p_1 = (SELECT CAST(ROUND(COUNT(DISTINCT user_id) * (1 + (0.2 * 3) / 3)) AS INT64) AS planned_mau
FROM project_id.analytics.user_actions
WHERE date BETWEEN '2025-09-01' AND '2025-09-30' AND segmentation = '1+');
SET p_l_1 = (SELECT CAST(ROUND(COUNT(DISTINCT user_id) * POWER(1 + POWER(1.2, 1 / 3) - 1, 3)) AS INT64) AS planned_mau
FROM project_id.analytics.user_actions
WHERE date BETWEEN '2025-09-01' AND '2025-09-30' AND segmentation = '1+' AND region = 'RegionA');
SET p_n_1 = (SELECT CAST(ROUND(COUNT(DISTINCT user_id) * POWER(1 + POWER(1.2, 1 / 3) - 1, 3)) AS INT64) AS planned_mau
FROM project_id.analytics.user_actions
WHERE date BETWEEN '2025-09-01' AND '2025-09-30' AND segmentation = '1+' AND region = 'RegionB');
SET p_n_60 = (SELECT CAST(ROUND(COUNT(DISTINCT user_id) * POWER(1 + POWER(1.15, 1 / 3) - 1, 3)) AS INT64) AS planned_mau
FROM project_id.analytics.user_actions
WHERE date BETWEEN '2025-09-01' AND '2025-09-30' AND segmentation = '60+' AND region = 'RegionB');
SET p_l_60 = (SELECT CAST(ROUND(COUNT(DISTINCT user_id) * POWER(1 + POWER(1.15, 1 / 3) - 1, 3)) AS INT64) AS planned_mau
FROM project_id.analytics.user_actions
WHERE date BETWEEN '2025-09-01' AND '2025-09-30' AND segmentation = '60+' AND region = 'RegionA');
SET p_60 = (SELECT CAST(ROUND(COUNT(DISTINCT user_id) * (1 + (0.15 * 3) / 3)) AS INT64) AS planned_mau
FROM project_id.analytics.user_actions
WHERE date BETWEEN '2025-09-01' AND '2025-09-30' AND segmentation = '60+');
---table for users's lack which is related with event_tracking
--incremental updating
DELETE FROM project_id.user_lack.user_lack_event_view WHERE DATE(event_time) = CURRENT_DATE() - 1;
INSERT INTO project_id.user_lack.user_lack_event_view
SELECT event_time, user_id
FROM project_id.event_tracking.event_android_new
WHERE DATE(event_time) = CURRENT_DATE() - 1 AND event_type = 'popup_shown' AND JSON_VALUE(event_properties, '$.popupType') IN ('insufficientFundsPopup', 'inactiveVideoPopupPrivate', 'inactiveVideoPopup', 'insufficientFundsPopupPrivate', 'insufficientFundsPopupPrivate') AND JSON_VALUE(event_properties, '$.entry') LIKE '%notenough%' AND DATE(event_date) >= CURRENT_DATE() - 3
GROUP BY all
UNION DISTINCT
SELECT event_time, user_id
FROM project_id.event_tracking.event_ios_new
WHERE DATE(event_time) = CURRENT_DATE() - 1 AND event_type = 'pay_screen_shown' AND JSON_VALUE(event_properties, '$.popupType') IN ('inactive_video_popups', 'first_time_insufficient_popup', 'active_video_popups') AND (JSON_VALUE(event_properties, '$.entry') LIKE '%notenough%' OR JSON_VALUE(event_properties, '$.entry') LIKE '%not_enough%') AND DATE(event_date) >= CURRENT_DATE() - 3
GROUP BY all;
----------public visible
---adding the new date; incremental update;
DELETE FROM project_id.analytics.public_visible WHERE date = CURRENT_DATE() - 1;
INSERT INTO project_id.analytics.public_visible
SELECT DATE(event_date) AS date, user_id
FROM project_id.event_tracking.event_android_new
WHERE JSON_VALUE(user_properties, '$.visibility') = 'public_visible' AND event_date = CURRENT_DATE() - 1 AND event_type IN ('search_shown', 'private_confirm_pressed', 'user_search_shown', 'private_call_pressed', 'private_confirm_pressed', 'startchat_shown', 'start_video')
GROUP BY 1, 2
UNION DISTINCT
SELECT DATE(event_date) AS date, user_id
FROM project_id.event_tracking.event_ios_new
WHERE JSON_VALUE(user_properties, '$.visibility') = 'public_visible' AND event_date = CURRENT_DATE() - 1 AND event_type IN ('search_shown', 'private_confirm_pressed', 'user_search_shown', 'private_call_pressed', 'private_confirm_pressed', 'startchat_shown', 'start_video')
GROUP BY 1, 2
UNION DISTINCT
SELECT DATE(event_date) AS date, CAST(user_id AS INT64)
FROM project_id.event_tracking.event_web
WHERE JSON_VALUE(user_properties, '$.visibility') = 'public_visible' AND event_date = CURRENT_DATE() - 1 AND event_type IN ('search_shown', 'private_confirm_pressed', 'user_search_shown', 'private_call_pressed', 'private_confirm_pressed', 'startchat_shown', 'start_video')
GROUP BY 1, 2
ORDER BY 1, 2;
--Groups with timezones
--Check tz database every half year for changes
TRUNCATE TABLE project_id.analytics.groups_timezones;
INSERT INTO project_id.analytics.groups_timezones
SELECT group_id, a.country_code, timezone
FROM (
  SELECT group_id, country_code
  FROM (
    SELECT group_id, country_code, COUNT(DISTINCT user_id) AS users
    FROM (
      SELECT h.user_id, group_id, country_code
      FROM `mysql.user_role` h
      LEFT JOIN `mysql.user_ip_country` c ON c.user_id = h.user_id
      ORDER BY 1, 2, 3
    )
    GROUP BY 1, 2
    HAVING country_code IS NOT NULL
    ORDER BY 1, 3 DESC
  )
  QUALIFY ROW_NUMBER() OVER (PARTITION BY group_id ORDER BY users DESC) = 1
  ORDER BY 1, 2
) a
LEFT JOIN `project_id.analytics.timezones` c ON c.country_code = a.country_code
ORDER BY 1, 2, 3;
--Table similar to `mysql.group` but also with referrals to simplify further calculations
TRUNCATE TABLE `analytics.group`;
INSERT INTO `analytics.group`
SELECT id, created_ts, CASE WHEN cont.country IS NOT NULL THEN cont.country
WHEN a.country = 'Viet Nam' THEN 'Vietnam'
WHEN a.country LIKE 'Venezuela, Bolivarian Republic%' THEN 'Venezuela' ELSE a.country END AS country,
lead_type, source, test_type, person, registration_date, is_core
FROM `mysql.group` a
LEFT JOIN `mysql.countries_continents` cont ON cont.iso_2 = a.country
UNION ALL
SELECT a.group_id AS id,
TIMESTAMP(created_at) AS created_ts, cont.country, NULL AS lead_type, NULL AS source,
NULL AS test_type, NULL AS person, NULL AS registration_date, NULL AS is_core
FROM (
  SELECT DISTINCT group_id
  FROM `mysql.user_role` s
  LEFT JOIN `mysql.group` a ON a.id = s.group_id
  WHERE a.id IS NULL
) a
LEFT JOIN `mysql.user_role` s ON s.user_id = a.group_id
LEFT JOIN `mysql.user_ip_country` ip ON ip.user_id = a.group_id
LEFT JOIN `mysql.countries_continents` cont ON cont.iso_2 = ip.country_code;
--action dau
--table which saves all the states of the users (all, 1+, 5+ etc.)
DELETE FROM project_id.analytics.user_actions WHERE date = CURRENT_DATE() - 1;
INSERT INTO project_id.analytics.user_actions
SELECT date, group_id, country, user_id, '60+' AS segmentation, CASE WHEN country IN ('Country1', 'Country2', 'Country3', 'Country4', 'Country5', 'Country6', 'Country7', 'Country8', 'Country9', 'Country10',
'Country11', 'Country12', 'Country13', 'Country14',
'Country15', 'Country16', 'Country17', 'Country18',
'Country19', 'Country20', 'Country21', 'Country22',
'Country23', 'Country24', 'Country25', 'Country26', 'Country27', 'Country28', 'Country29', 'Country30') THEN 'RegionA' ELSE 'RegionB' END AS region
FROM (
  SELECT DATE(ub.created_at) AS date, group_id, country, ub.user_id, COUNT(*) AS nums
  FROM project_id.mysql.users u
  LEFT JOIN project_id.mysql.user_role s ON s.user_id = u.id
  LEFT JOIN project_id.mysql.user_ip_country ip ON ip.user_id = u.id
  LEFT JOIN project_id.mysql.countries_continents cont ON cont.iso_2 = ip.country_code
  LEFT JOIN project_id.mysql.user_balance_history ub ON ub.user_id = u.id
  WHERE roles LIKE '%ROLE%' AND JSON_VALUE(description, '$.action') IN ('enroll', 'enroll_private', 'gift_received') AND DATE(ub.created_at) = CURRENT_DATE() - 1
  GROUP BY 1, 2, 3, 4
  ORDER BY 1, 2
)
WHERE nums >= 60
UNION ALL
SELECT date, group_id, country, user_id, '20+' AS segmentation, CASE WHEN country IN ('Country1', 'Country2', 'Country3', 'Country4', 'Country5', 'Country6', 'Country7', 'Country8', 'Country9', 'Country10',
'Country11', 'Country12', 'Country13', 'Country14',
'Country15', 'Country16', 'Country17', 'Country18',
'Country19', 'Country20', 'Country21', 'Country22',
'Country23', 'Country24', 'Country25', 'Country26', 'Country27', 'Country28', 'Country29', 'Country30') THEN 'RegionA' ELSE 'RegionB' END AS region
FROM (
  SELECT DATE(ub.created_at) AS date, group_id, country, ub.user_id, COUNT(*) AS nums
  FROM project_id.mysql.users u
  LEFT JOIN project_id.mysql.user_role s ON s.user_id = u.id
  LEFT JOIN project_id.mysql.user_ip_country ip ON ip.user_id = u.id
  LEFT JOIN project_id.mysql.countries_continents cont ON cont.iso_2 = ip.country_code
  LEFT JOIN project_id.mysql.user_balance_history ub ON ub.user_id = u.id
  WHERE roles LIKE '%ROLE%' AND JSON_VALUE(description, '$.action') IN ('enroll', 'enroll_private', 'gift_received') AND DATE(ub.created_at) = CURRENT_DATE() - 1
  GROUP BY 1, 2, 3, 4
  ORDER BY 1, 2
)
WHERE nums >= 20
UNION ALL
SELECT date, group_id, country, user_id, '5+' AS segmentation, CASE WHEN country IN ('Country1', 'Country2', 'Country3', 'Country4', 'Country5', 'Country6', 'Country7', 'Country8', 'Country9', 'Country10',
'Country11', 'Country12', 'Country13', 'Country14',
'Country15', 'Country16', 'Country17', 'Country18',
'Country19', 'Country20', 'Country21', 'Country22',
'Country23', 'Country24', 'Country25', 'Country26', 'Country27', 'Country28', 'Country29', 'Country30') THEN 'RegionA' ELSE 'RegionB' END AS region
FROM (
  SELECT DATE(ub.created_at) AS date, group_id, country, ub.user_id, COUNT(*) AS nums
  FROM project_id.mysql.users u
  LEFT JOIN project_id.mysql.user_role s ON s.user_id = u.id
  LEFT JOIN project_id.mysql.user_ip_country ip ON ip.user_id = u.id
  LEFT JOIN project_id.mysql.countries_continents cont ON cont.iso_2 = ip.country_code
  LEFT JOIN project_id.mysql.user_balance_history ub ON ub.user_id = u.id
  WHERE roles LIKE '%ROLE%' AND JSON_VALUE(description, '$.action') IN ('enroll', 'enroll_private', 'gift_received') AND DATE(ub.created_at) = CURRENT_DATE() - 1
  GROUP BY 1, 2, 3, 4
  ORDER BY 1, 2
)
WHERE nums >= 5
UNION ALL
SELECT date, group_id, country, user_id, '1+' AS segmentation, CASE WHEN country IN ('Country1', 'Country2', 'Country3', 'Country4', 'Country5', 'Country6', 'Country7', 'Country8', 'Country9', 'Country10',
'Country11', 'Country12', 'Country13', 'Country14',
'Country15', 'Country16', 'Country17', 'Country18',
'Country19', 'Country20', 'Country21', 'Country22',
'Country23', 'Country24', 'Country25', 'Country26', 'Country27', 'Country28', 'Country29', 'Country30') THEN 'RegionA' ELSE 'RegionB' END AS region
FROM (
  SELECT DATE(ub.created_at) AS date, group_id, country, ub.user_id, COUNT(*) AS nums
  FROM project_id.mysql.users u
  LEFT JOIN project_id.mysql.user_role s ON s.user_id = u.id
  LEFT JOIN project_id.mysql.user_ip_country ip ON ip.user_id = u.id
  LEFT JOIN project_id.mysql.countries_continents cont ON cont.iso_2 = ip.country_code
  LEFT JOIN project_id.mysql.user_balance_history ub ON ub.user_id = u.id
  WHERE roles LIKE '%ROLE%' AND JSON_VALUE(description, '$.action') IN ('enroll', 'enroll_private', 'gift_received') AND DATE(ub.created_at) = CURRENT_DATE() - 1
  GROUP BY 1, 2, 3, 4
  ORDER BY 1, 2
)
WHERE nums >= 1
UNION ALL
SELECT date, group_id, country, to_user_id, 'All' AS segmentation, CASE WHEN country IN ('Country1', 'Country2', 'Country3', 'Country4', 'Country5', 'Country6', 'Country7', 'Country8', 'Country9', 'Country10',
'Country11', 'Country12', 'Country13', 'Country14',
'Country15', 'Country16', 'Country17', 'Country18',
'Country19', 'Country20', 'Country21', 'Country22',
'Country23', 'Country24', 'Country25', 'Country26', 'Country27', 'Country28', 'Country29', 'Country30') THEN 'RegionA' ELSE 'RegionB' END AS region
FROM (
  SELECT DATE(ub.created_at) AS date, group_id, country, ub.to_user_id
  FROM project_id.mysql.user_role s
  LEFT JOIN project_id.mysql.user_ip_country ip ON ip.user_id = s.user_id
  LEFT JOIN project_id.mysql.countries_continents cont ON cont.iso_2 = ip.country_code
  LEFT JOIN project_id.mysql.activity_log ub ON ub.to_user_id = s.user_id
  WHERE duration > 0 AND DATE(ub.created_at) = CURRENT_DATE() - 1
  GROUP BY 1, 2, 3, 4
  ORDER BY 1, 2
);
-----groups & new users
TRUNCATE TABLE project_id.analytics.groups;
INSERT INTO project_id.analytics.groups
SELECT date, user_id, group_id, country, region, source, test_type, person, is_core, registration_date, lead_type,
CASE WHEN registration_date >= CURRENT_DATE() - 15 THEN 'new' ELSE 'old' END AS new_group, region_user, country_user, event
FROM (
  SELECT DATE(s.created_at) AS date, s.user_id, s.group_id,
  a.country,
  CASE WHEN cont1.region = 'RegionX' THEN 'RegionX' ELSE cont1.continent END AS region, source, test_type, person, is_core,
  COALESCE(registration_date, DATE(created_ts)) AS registration_date, lead_type,
  CASE WHEN cont1.country IN ('Country1', 'Country2', 'Country3', 'Country4', 'Country5', 'Country6', 'Country7', 'Country8', 'Country9', 'Country10',
'Country11', 'Country12', 'Country13', 'Country14',
'Country15', 'Country16', 'Country17', 'Country18',
'Country19', 'Country20', 'Country21', 'Country22',
'Country23', 'Country24', 'Country25', 'Country26', 'Country27', 'Country28', 'Country29', 'Country30') THEN 'RegionA' ELSE 'RegionB' END AS region_user, cont1.country AS country_user, 'Registrations' AS event
  FROM project_id.mysql.user_role s
  LEFT JOIN `analytics.group` a ON s.group_id = a.id
  LEFT JOIN project_id.mysql.user_ip_country ip ON ip.user_id = s.user_id
  LEFT JOIN project_id.mysql.countries_continents cont1 ON cont1.iso_2 = ip.country_code
  WHERE DATE(s.created_at) BETWEEN '2024-10-01' AND CURRENT_DATE() - 1
)
UNION ALL
SELECT date, user_id, group_id, country, region, source, test_type, person, is_core, registration_date, lead_type,
CASE WHEN registration_date >= CURRENT_DATE() - 15 THEN 'new' ELSE 'old' END AS new_group, region_user, country_user, event
FROM (
  SELECT DATE(s.created_at) AS date, s.user_id, s.group_id,
  a.country,
  CASE WHEN cont1.region = 'RegionX' THEN 'RegionX' ELSE cont1.continent END AS region, source, test_type, person, is_core,
  COALESCE(registration_date, DATE(created_ts)) AS registration_date, lead_type,
  CASE WHEN cont1.country IN ('Country1', 'Country2', 'Country3', 'Country4', 'Country5', 'Country6', 'Country7', 'Country8', 'Country9', 'Country10',
'Country11', 'Country12', 'Country13', 'Country14',
'Country15', 'Country16', 'Country17', 'Country18',
'Country19', 'Country20', 'Country21', 'Country22',
'Country23', 'Country24', 'Country25', 'Country26', 'Country27', 'Country28', 'Country29', 'Country30') THEN 'RegionA' ELSE 'RegionB' END AS region_user, cont1.country AS country_user, 'Activated' AS event
  FROM project_id.mysql.user_role s
  LEFT JOIN `analytics.group` a ON a.id = s.group_id
  LEFT JOIN project_id.mysql.user_ip_country ip ON ip.user_id = s.user_id
  LEFT JOIN project_id.mysql.countries_continents cont1 ON cont1.iso_2 = ip.country_code
  LEFT JOIN project_id.analytics.user_actions vp ON vp.user_id = s.user_id AND vp.segmentation = 'All'
  WHERE DATE(s.created_at) BETWEEN '2024-10-01' AND CURRENT_DATE() - 1 AND vp.user_id IS NOT NULL AND DATE(vp.date) BETWEEN '2024-10-01' AND CURRENT_DATE() - 1
  GROUP BY ALL
)
UNION ALL
SELECT date, user_id, group_id, country, region, source, test_type, person, is_core, registration_date, lead_type,
CASE WHEN registration_date >= CURRENT_DATE() - 15 THEN 'new' ELSE 'old' END AS new_group, region_user, country_user, event
FROM (
  SELECT DATE(s.created_at) AS date, s.user_id, s.group_id,
  a.country,
  CASE WHEN cont1.region = 'RegionX' THEN 'RegionX' ELSE cont1.continent END AS region, source, test_type, person, is_core,
  COALESCE(registration_date, DATE(created_ts)) AS registration_date, lead_type,
  CASE WHEN cont1.country IN ('Country1', 'Country2', 'Country3', 'Country4', 'Country5', 'Country6', 'Country7', 'Country8', 'Country9', 'Country10',
'Country11', 'Country12', 'Country13', 'Country14',
'Country15', 'Country16', 'Country17', 'Country18',
'Country19', 'Country20', 'Country21', 'Country22',
'Country23', 'Country24', 'Country25', 'Country26', 'Country27', 'Country28', 'Country29', 'Country30') THEN 'RegionA' ELSE 'RegionB' END AS region_user, cont1.country AS country_user, 'Churned' AS event
  FROM project_id.mysql.user_role s
  LEFT JOIN `analytics.group` a ON a.id = s.group_id
  LEFT JOIN project_id.mysql.user_ip_country ip ON ip.user_id = s.user_id
  LEFT JOIN project_id.mysql.countries_continents cont1 ON cont1.iso_2 = ip.country_code
  LEFT JOIN (
    SELECT date, user_id, last_activity, CASE WHEN last_activity IS NULL AND DATE_DIFF(CURRENT_DATE() - 1, date, DAY) >= 14 THEN 'Churned'
    WHEN last_activity IS NOT NULL AND DATE_DIFF(CURRENT_DATE() - 1, DATE(last_activity), DAY) >= 14 THEN 'Churned' ELSE NULL END AS churned
    FROM (
      SELECT DATE(s.created_at) AS date, s.user_id, MAX(vp.date) AS last_activity
      FROM project_id.mysql.user_role s
      LEFT JOIN project_id.analytics.user_actions vp ON vp.user_id = s.user_id AND vp.segmentation = 'All'
      WHERE DATE(s.created_at) BETWEEN '2024-10-01' AND CURRENT_DATE() - 1
      GROUP BY ALL
    )
    ORDER BY date
  ) churned ON churned.user_id = s.user_id
  LEFT JOIN project_id.analytics.user_actions vp ON vp.user_id = s.user_id AND vp.segmentation = 'All'
  WHERE DATE(s.created_at) BETWEEN '2024-10-01' AND CURRENT_DATE() - 1 AND DATE(vp.date) BETWEEN '2024-10-01' AND CURRENT_DATE() - 1
  AND churned.churned = 'Churned' AND vp.user_id IS NOT NULL
  GROUP BY ALL
)
UNION ALL
SELECT date, user_id, group_id, country, region, source, test_type, person, is_core, registration_date, lead_type,
CASE WHEN registration_date >= CURRENT_DATE() - 15 THEN 'new' ELSE 'old' END AS new_group, region_user, country_user, event
FROM (
  SELECT DATE(s.created_at) AS date, s.user_id, s.group_id,
  a.country,
  CASE WHEN cont1.region = 'RegionX' THEN 'RegionX' ELSE cont1.continent END AS region, source, test_type, person, is_core,
  COALESCE(registration_date, DATE(created_ts)) AS registration_date, lead_type,
  CASE WHEN cont1.country IN ('Country1', 'Country2', 'Country3', 'Country4', 'Country5', 'Country6', 'Country7', 'Country8', 'Country9', 'Country10',
'Country11', 'Country12', 'Country13', 'Country14',
'Country15', 'Country16', 'Country17', 'Country18',
'Country19', 'Country20', 'Country21', 'Country22',
'Country23', 'Country24', 'Country25', 'Country26', 'Country27', 'Country28', 'Country29', 'Country30') THEN 'RegionA' ELSE 'RegionB' END AS region_user, cont1.country AS country_user, 'Active' AS event
  FROM project_id.mysql.user_role s
  LEFT JOIN `analytics.group` a ON a.id = s.group_id
  LEFT JOIN project_id.mysql.user_ip_country ip ON ip.user_id = s.user_id
  LEFT JOIN project_id.mysql.countries_continents cont1 ON cont1.iso_2 = ip.country_code
  LEFT JOIN (
    SELECT date, user_id, last_activity, CASE WHEN last_activity IS NULL AND DATE_DIFF(CURRENT_DATE() - 1, date, DAY) >= 14 THEN 'Churned'
    WHEN last_activity IS NOT NULL AND DATE_DIFF(CURRENT_DATE() - 1, DATE(last_activity), DAY) >= 14 THEN 'Churned' ELSE NULL END AS churned
    FROM (
      SELECT DATE(s.created_at) AS date, s.user_id, MAX(vp.date) AS last_activity
      FROM project_id.mysql.user_role s
      LEFT JOIN project_id.analytics.user_actions vp ON vp.user_id = s.user_id AND vp.segmentation = 'All'
      WHERE DATE(s.created_at) BETWEEN '2024-10-01' AND CURRENT_DATE() - 1
      GROUP BY ALL
    )
    ORDER BY date
  ) churned ON churned.user_id = s.user_id
  LEFT JOIN project_id.analytics.user_actions vp ON vp.user_id = s.user_id AND vp.segmentation = 'All'
  WHERE DATE(s.created_at) BETWEEN '2024-10-01' AND CURRENT_DATE() - 1 AND churned.churned IS NULL AND vp.user_id IS NOT NULL AND DATE(vp.date) BETWEEN '2024-10-01' AND CURRENT_DATE() - 1
  GROUP BY ALL
)
UNION ALL
SELECT date, user_id, group_id, country, region, source, test_type, person, is_core, registration_date, lead_type,
CASE WHEN registration_date >= CURRENT_DATE() - 15 THEN 'new' ELSE 'old' END AS new_group, region_user, country_user, event
FROM (
  SELECT DATE(s.created_at) AS date, s.user_id, s.group_id,
  a.country,
  CASE WHEN cont1.region = 'RegionX' THEN 'RegionX' ELSE cont1.continent END AS region, source, test_type, person, is_core,
  COALESCE(registration_date, DATE(created_ts)) AS registration_date, lead_type,
  CASE WHEN cont1.country IN ('Country1', 'Country2', 'Country3', 'Country4', 'Country5', 'Country6', 'Country7', 'Country8', 'Country9', 'Country10',
'Country11', 'Country12', 'Country13', 'Country14',
'Country15', 'Country16', 'Country17', 'Country18',
'Country19', 'Country20', 'Country21', 'Country22',
'Country23', 'Country24', 'Country25', 'Country26', 'Country27', 'Country28', 'Country29', 'Country30') THEN 'RegionA' ELSE 'RegionB' END AS region_user, cont1.country AS country_user, 'Withdrawn' AS event
  FROM project_id.mysql.user_role s
  LEFT JOIN `analytics.group` a ON a.id = s.group_id
  LEFT JOIN project_id.mysql.user_ip_country ip ON ip.user_id = s.user_id
  LEFT JOIN project_id.mysql.countries_continents cont1 ON cont1.iso_2 = ip.country_code
  LEFT JOIN (
    SELECT user, status, usd, date
    FROM (
      SELECT User, Status, `USD Amount` AS usd, `Approved At` AS date
      FROM project_id.users.system_payments withdraw
      LEFT JOIN project_id.mysql.users u ON withdraw.User = u.id
      LEFT JOIN project_id.mysql.user_ip_country ip ON withdraw.User = ip.user_id
      LEFT JOIN project_id.mysql.countries_continents cont ON cont.iso_2 = ip.country_code
      ORDER BY user ASC, date ASC
    )
    ORDER BY 4 ASC
  ) withdrawn ON withdrawn.user = s.user_id
  LEFT JOIN project_id.analytics.user_actions vp ON vp.user_id = s.user_id AND vp.segmentation = 'All'
  WHERE DATE(s.created_at) BETWEEN '2024-10-01' AND CURRENT_DATE() - 1 AND withdrawn.user IS NOT NULL AND vp.user_id IS NOT NULL AND DATE(vp.date) BETWEEN '2024-10-01' AND CURRENT_DATE() - 1
  GROUP BY ALL
);
-------groups for top dash
---Create table since we create report on weekly basis
---and look at users who had activity by the end of the week
TRUNCATE TABLE project_id.analytics.groups_for_top;
INSERT INTO project_id.analytics.groups_for_top
SELECT date, user_id, group_id, country, region, source, test_type, person, is_core, registration_date, lead_type,
CASE WHEN registration_date >= CURRENT_DATE() - 15 THEN 'new' ELSE 'old' END AS new_group, event
FROM (
  SELECT DATE(s.created_at) AS date, s.user_id, s.group_id, a.country,
  CASE WHEN cont1.region = 'RegionX' THEN 'RegionX' ELSE cont1.continent END AS region, source, test_type, person, is_core,
  COALESCE(registration_date, DATE(created_ts)) AS registration_date, lead_type,
  'Registrations' AS event
  FROM project_id.mysql.user_role s
  LEFT JOIN `analytics.group` a ON a.id = s.group_id
  LEFT JOIN project_id.mysql.user_ip_country ip ON ip.user_id = s.user_id
  LEFT JOIN project_id.mysql.countries_continents cont1 ON cont1.iso_2 = ip.country_code
  WHERE DATE(s.created_at) BETWEEN '2024-10-01' AND CURRENT_DATE() - 1
)
UNION ALL
SELECT date, user_id, group_id, country, region, source, test_type, person, is_core, registration_date, lead_type,
CASE WHEN registration_date >= CURRENT_DATE() - 15 THEN 'new' ELSE 'old' END AS new_group, event
FROM (
  SELECT DATE(s.created_at) AS date, s.user_id, s.group_id,
  a.country, CASE WHEN cont1.region = 'RegionX' THEN 'RegionX' ELSE cont1.continent END AS region, source, test_type, person, is_core,
  COALESCE(registration_date, DATE(created_ts)) AS registration_date, lead_type,
  'Activated' AS event
  FROM project_id.mysql.user_role s
  LEFT JOIN `analytics.group` a ON a.id = s.group_id
  LEFT JOIN project_id.mysql.user_ip_country ip ON ip.user_id = s.user_id
  LEFT JOIN project_id.mysql.countries_continents cont1 ON cont1.iso_2 = ip.country_code
  LEFT JOIN project_id.analytics.user_actions vp ON vp.user_id = s.user_id AND DATE(vp.date) BETWEEN DATE(s.created_at)
  AND DATE(DATE_TRUNC(DATE(s.created_at), WEEK(MONDAY)) + INTERVAL 6 DAY) AND vp.segmentation = 'All'
  WHERE DATE(s.created_at) BETWEEN '2024-10-01' AND CURRENT_DATE() - 1 AND vp.user_id IS NOT NULL AND DATE(vp.date) BETWEEN '2024-10-01' AND CURRENT_DATE() - 1
  GROUP BY ALL
);
---The same report as groups, but we use user_actions as the main
---In this table only count activated users
TRUNCATE TABLE project_id.analytics.groups_num;
INSERT INTO project_id.analytics.groups_num
SELECT date, user_id, group_id, country, region, source, test_type, person, is_core, registration_date, lead_type,
CASE WHEN registration_date >= CURRENT_DATE() - 15 THEN 'new' ELSE 'old' END AS new_group, segmentation, region_user, country_user
FROM (
  SELECT DATE(s.created_at) AS date, s.user_id, s.group_id,
  a.country, CASE WHEN cont1.region = 'RegionX' THEN 'RegionX' ELSE cont1.continent END AS region, source, test_type, person, is_core,
  COALESCE(registration_date, DATE(created_ts)) AS registration_date, lead_type, CASE WHEN segmentation = 'All' THEN 'Activated' ELSE segmentation END AS segmentation, CASE WHEN cont1.country IN ('Country1', 'Country2', 'Country3', 'Country4', 'Country5', 'Country6', 'Country7', 'Country8', 'Country9', 'Country10',
'Country11', 'Country12', 'Country13', 'Country14',
'Country15', 'Country16', 'Country17', 'Country18',
'Country19', 'Country20', 'Country21', 'Country22',
'Country23', 'Country24', 'Country25', 'Country26', 'Country27', 'Country28', 'Country29', 'Country30') THEN 'RegionA' ELSE 'RegionB' END AS region_user, cont1.country AS country_user
  FROM project_id.mysql.user_role s
  LEFT JOIN `analytics.group` a ON a.id = s.group_id
  LEFT JOIN project_id.mysql.user_ip_country ip ON ip.user_id = s.user_id
  LEFT JOIN project_id.mysql.countries_continents cont1 ON cont1.iso_2 = ip.country_code
  LEFT JOIN project_id.analytics.user_actions act ON act.user_id = s.user_id
  WHERE DATE(s.created_at) BETWEEN '2024-10-01' AND CURRENT_DATE() - 1 AND segmentation IS NOT NULL
  GROUP BY ALL
  ORDER BY date DESC, group_id DESC
);
--new groups
TRUNCATE TABLE project_id.analytics.new_groups;
INSERT INTO project_id.analytics.new_groups
SELECT DATE(COALESCE(registration_date, DATE(created_ts))) AS registration_date, id AS group_id,
CASE WHEN cont.country IS NOT NULL THEN cont.country
WHEN a.country = 'Viet Nam' THEN 'Vietnam'
WHEN a.country LIKE 'Venezuela, Bolivarian Republic%' THEN 'Venezuela' ELSE a.country END AS country, CASE WHEN cont.region = 'RegionX' THEN 'RegionX' ELSE cont.continent END AS region, source, test_type, person, is_core, lead_type, CASE WHEN COALESCE(registration_date, DATE(created_ts)) >= CURRENT_DATE() - 15 THEN 'new' ELSE 'old' END AS new_group, region_user, country_user
FROM project_id.mysql.group a
LEFT JOIN project_id.mysql.countries_continents cont ON cont.iso_2 = a.country
LEFT JOIN (
  SELECT CASE WHEN country IN ('Country1', 'Country2', 'Country3', 'Country4', 'Country5', 'Country6', 'Country7', 'Country8', 'Country9', 'Country10',
'Country11', 'Country12', 'Country13', 'Country14',
'Country15', 'Country16', 'Country17', 'Country18',
'Country19', 'Country20', 'Country21', 'Country22',
'Country23', 'Country24', 'Country25', 'Country26', 'Country27', 'Country28', 'Country29', 'Country30') THEN 'RegionA' ELSE 'RegionB' END AS region_user, country AS country_user, group_id, COUNT(DISTINCT s.user_id) AS users
  FROM project_id.mysql.user_role s
  LEFT JOIN project_id.mysql.user_ip_country ip ON ip.user_id = s.user_id
  LEFT JOIN project_id.mysql.countries_continents cont1 ON cont1.iso_2 = ip.country_code
  WHERE DATE(created_at) BETWEEN '2024-10-01' AND CURRENT_DATE() - 1
  GROUP BY 1, 2, 3
  ORDER BY 1, 2, 3
) h ON h.group_id = a.id
WHERE DATE(registration_date) BETWEEN '2024-10-01' AND CURRENT_DATE() - 1;
--New/Old groups & new users
TRUNCATE TABLE project_id.analytics.new_old_groups;
INSERT INTO project_id.analytics.new_old_groups
SELECT date, user_id, group_id, country, region, source, test_type, person, is_core, registration_date, lead_type,
CASE WHEN DATE_DIFF(date, registration_date, DAY) <= 14 THEN 'new' ELSE 'old' END AS new_group, region_user, country_user
FROM (
  SELECT DATE(s.created_at) AS date, s.user_id, s.group_id,
  a.country,
  CASE WHEN cont1.region = 'RegionX' THEN 'RegionX' ELSE cont1.continent END AS region, source, test_type, person, is_core,
  COALESCE(registration_date, DATE(created_ts)) AS registration_date, lead_type,
  CASE WHEN cont1.country IN ('Country1', 'Country2', 'Country3', 'Country4', 'Country5', 'Country6', 'Country7', 'Country8', 'Country9', 'Country10',
'Country11', 'Country12', 'Country13', 'Country14',
'Country15', 'Country16', 'Country17', 'Country18',
'Country19', 'Country20', 'Country21', 'Country22',
'Country23', 'Country24', 'Country25', 'Country26', 'Country27', 'Country28', 'Country29', 'Country30') THEN 'RegionA' ELSE 'RegionB' END AS region_user, cont1.country AS country_user
  FROM project_id.mysql.user_role s
  LEFT JOIN `analytics.group` a ON a.id = s.group_id
  LEFT JOIN project_id.mysql.user_ip_country ip ON ip.user_id = s.user_id
  LEFT JOIN project_id.mysql.countries_continents cont1 ON cont1.iso_2 = ip.country_code
  WHERE DATE(s.created_at) BETWEEN '2024-10-01' AND CURRENT_DATE() - 1
  ORDER BY 1, 2, 3, 4
)
ORDER BY 1, 2, 3, 4;
----mau
TRUNCATE TABLE project_id.analytics.mau;
INSERT INTO project_id.analytics.mau
SELECT d.date, d.mau AS fact_mau, p_1 AS planned_mau, EXTRACT(DAY FROM d.date) AS day_number, CASE WHEN ratio IS NOT NULL THEN ROUND(d.mau / ratio) END AS forecast_mau
FROM (
  SELECT a.date, COUNT(DISTINCT b.user_id) AS mau
  FROM (SELECT DISTINCT date
        FROM project_id.analytics.user_actions
        WHERE EXTRACT(YEAR FROM date) = EXTRACT(YEAR FROM CURRENT_DATE() - 1) AND EXTRACT(MONTH FROM date) = EXTRACT(MONTH FROM CURRENT_DATE() - 1)
       ) a
  CROSS JOIN project_id.analytics.user_actions b
  WHERE b.date <= a.date
  AND b.date >= DATE_TRUNC(CURRENT_DATE() - 1, MONTH)
  AND segmentation = '1+'
  GROUP BY a.date
  ORDER BY a.date
) d
LEFT JOIN (
  SELECT date, mau, mau / total AS ratio
  FROM (SELECT a.date, COUNT(DISTINCT b.user_id) AS mau
        FROM (
          SELECT DISTINCT date
          FROM project_id.analytics.user_actions
          WHERE date BETWEEN (
              DATE_TRUNC( CASE WHEN EXTRACT(DAY FROM LAST_DAY(DATE_SUB(CURRENT_DATE() - 1, INTERVAL 1 MONTH))) >=
              EXTRACT(DAY FROM LAST_DAY(CURRENT_DATE() - 1))
              THEN DATE_SUB(CURRENT_DATE() - 1, INTERVAL 1 MONTH)
              ELSE DATE_SUB(CURRENT_DATE() - 1, INTERVAL 2 MONTH)
              END, MONTH )
            ) AND (
              LAST_DAY(
              CASE WHEN EXTRACT(DAY FROM LAST_DAY(DATE_SUB(CURRENT_DATE() - 1, INTERVAL 1 MONTH))) >=
              EXTRACT(DAY FROM LAST_DAY(CURRENT_DATE() - 1))
              THEN DATE_SUB(CURRENT_DATE() - 1, INTERVAL 1 MONTH)
              ELSE DATE_SUB(CURRENT_DATE() - 1, INTERVAL 2 MONTH)
              END)
            )
        ) a
        CROSS JOIN project_id.analytics.user_actions b
        WHERE b.date <= a.date
        AND b.date >= DATE_TRUNC(CASE
          WHEN EXTRACT(DAY FROM LAST_DAY(DATE_SUB(CURRENT_DATE() - 1, INTERVAL 1 MONTH))) >=
          EXTRACT(DAY FROM LAST_DAY(CURRENT_DATE() - 1))
          THEN DATE_SUB(CURRENT_DATE() - 1, INTERVAL 1 MONTH)
          ELSE DATE_SUB(CURRENT_DATE() - 1, INTERVAL 2 MONTH)
          END, MONTH
        )
        AND segmentation = '1+'
        GROUP BY a.date
        ORDER BY a.date
       )
  CROSS JOIN (
    SELECT COUNT(DISTINCT user_id) AS total
    FROM project_id.analytics.user_actions
    WHERE date BETWEEN (
        DATE_TRUNC( CASE WHEN EXTRACT(DAY FROM LAST_DAY(DATE_SUB(CURRENT_DATE() - 1, INTERVAL 1 MONTH))) >=
        EXTRACT(DAY FROM LAST_DAY(CURRENT_DATE() - 1))
        THEN DATE_SUB(CURRENT_DATE() - 1, INTERVAL 1 MONTH)
        ELSE DATE_SUB(CURRENT_DATE() - 1, INTERVAL 2 MONTH)
        END, MONTH )
      ) AND (
        LAST_DAY(
        CASE WHEN EXTRACT(DAY FROM LAST_DAY(DATE_SUB(CURRENT_DATE() - 1, INTERVAL 1 MONTH))) >=
        EXTRACT(DAY FROM LAST_DAY(CURRENT_DATE() - 1))
        THEN DATE_SUB(CURRENT_DATE() - 1, INTERVAL 1 MONTH)
        ELSE DATE_SUB(CURRENT_DATE() - 1, INTERVAL 2 MONTH)
        END)
      ) AND segmentation = '1+'
  )
  ORDER BY 1
) rat ON EXTRACT(DAY FROM rat.date) = EXTRACT(DAY FROM d.date)
ORDER BY date;
------mau 60
TRUNCATE TABLE project_id.analytics.mau_60;
INSERT INTO project_id.analytics.mau_60
SELECT d.date, d.mau AS fact_mau, p_60 AS planned_mau, EXTRACT(DAY FROM d.date) AS day_number, CASE WHEN ratio IS NOT NULL THEN ROUND(d.mau / ratio) END AS forecast_mau
FROM (
  SELECT a.date, COUNT(DISTINCT b.user_id) AS mau
  FROM (SELECT DISTINCT date
        FROM project_id.analytics.user_actions
        WHERE EXTRACT(YEAR FROM date) = EXTRACT(YEAR FROM CURRENT_DATE() - 1) AND EXTRACT(MONTH FROM date) = EXTRACT(MONTH FROM CURRENT_DATE() - 1)
       ) a
  CROSS JOIN project_id.analytics.user_actions b
  WHERE b.date <= a.date
  AND b.date >= DATE_TRUNC(CURRENT_DATE() - 1, MONTH)
  AND segmentation = '60+'
  GROUP BY a.date
  ORDER BY a.date
) d
LEFT JOIN (
  SELECT date, mau, mau / total AS ratio
  FROM (SELECT a.date, COUNT(DISTINCT b.user_id) AS mau
        FROM (
          SELECT DISTINCT date
          FROM project_id.analytics.user_actions
          WHERE date BETWEEN (
              DATE_TRUNC( CASE WHEN EXTRACT(DAY FROM LAST_DAY(DATE_SUB(CURRENT_DATE() - 1, INTERVAL 1 MONTH))) >=
              EXTRACT(DAY FROM LAST_DAY(CURRENT_DATE() - 1))
              THEN DATE_SUB(CURRENT_DATE() - 1, INTERVAL 1 MONTH)
              ELSE DATE_SUB(CURRENT_DATE() - 1, INTERVAL 2 MONTH)
              END, MONTH )
            ) AND (
              LAST_DAY(
              CASE WHEN EXTRACT(DAY FROM LAST_DAY(DATE_SUB(CURRENT_DATE() - 1, INTERVAL 1 MONTH))) >=
              EXTRACT(DAY FROM LAST_DAY(CURRENT_DATE() - 1))
              THEN DATE_SUB(CURRENT_DATE() - 1, INTERVAL 1 MONTH)
              ELSE DATE_SUB(CURRENT_DATE() - 1, INTERVAL 2 MONTH)
              END)
            )
        ) a
        CROSS JOIN project_id.analytics.user_actions b
        WHERE b.date <= a.date
        AND b.date >= DATE_TRUNC(CASE
          WHEN EXTRACT(DAY FROM LAST_DAY(DATE_SUB(CURRENT_DATE() - 1, INTERVAL 1 MONTH))) >=
          EXTRACT(DAY FROM LAST_DAY(CURRENT_DATE() - 1))
          THEN DATE_SUB(CURRENT_DATE() - 1, INTERVAL 1 MONTH)
          ELSE DATE_SUB(CURRENT_DATE() - 1, INTERVAL 2 MONTH)
          END, MONTH
        )
        AND segmentation = '60+'
        GROUP BY a.date
        ORDER BY a.date
       )
  CROSS JOIN (
    SELECT COUNT(DISTINCT user_id) AS total
    FROM project_id.analytics.user_actions
    WHERE date BETWEEN (
        DATE_TRUNC( CASE WHEN EXTRACT(DAY FROM LAST_DAY(DATE_SUB(CURRENT_DATE() - 1, INTERVAL 1 MONTH))) >=
        EXTRACT(DAY FROM LAST_DAY(CURRENT_DATE() - 1))
        THEN DATE_SUB(CURRENT_DATE() - 1, INTERVAL 1 MONTH)
        ELSE DATE_SUB(CURRENT_DATE() - 1, INTERVAL 2 MONTH)
        END, MONTH )
      ) AND (
        LAST_DAY(
        CASE WHEN EXTRACT(DAY FROM LAST_DAY(DATE_SUB(CURRENT_DATE() - 1, INTERVAL 1 MONTH))) >=
        EXTRACT(DAY FROM LAST_DAY(CURRENT_DATE() - 1))
        THEN DATE_SUB(CURRENT_DATE() - 1, INTERVAL 1 MONTH)
        ELSE DATE_SUB(CURRENT_DATE() - 1, INTERVAL 2 MONTH)
        END)
      ) AND segmentation = '60+'
  )
  ORDER BY 1
) rat ON EXTRACT(DAY FROM rat.date) = EXTRACT(DAY FROM d.date)
ORDER BY date;
--history mau
-- Step 1: Delete records from the current month in the history_mau table
DELETE FROM `project_id.analytics.history_mau` WHERE month = DATE_TRUNC(CURRENT_DATE() - 1, MONTH);
-- Step 2: Insert new data for the current month
INSERT INTO `project_id.analytics.history_mau` (month, segmentation, mau)
SELECT DATE_TRUNC(date, MONTH) AS month, segmentation, COUNT(DISTINCT user_id) AS mau
FROM project_id.analytics.user_actions
WHERE DATE_TRUNC(date, MONTH) = DATE_TRUNC(CURRENT_DATE() - 1, MONTH) AND segmentation IN ('1+', '60+')
GROUP BY month, segmentation
ORDER BY month DESC;
---mau RegionA 1+
TRUNCATE TABLE project_id.analytics.mau_l;
INSERT INTO project_id.analytics.mau_l
SELECT d.date, d.mau AS fact_mau, p_l_1 AS planned_mau, EXTRACT(DAY FROM d.date) AS day_number, CASE WHEN ratio IS NOT NULL THEN ROUND(d.mau / ratio) END AS forecast_mau
FROM (
  SELECT a.date, COUNT(DISTINCT b.user_id) AS mau
  FROM (SELECT DISTINCT date
        FROM project_id.analytics.user_actions
        WHERE EXTRACT(YEAR FROM date) = EXTRACT(YEAR FROM CURRENT_DATE() - 1) AND EXTRACT(MONTH FROM date) = EXTRACT(MONTH FROM CURRENT_DATE() - 1)
       ) a
  CROSS JOIN project_id.analytics.user_actions b
  WHERE b.date <= a.date
  AND b.date >= DATE_TRUNC(CURRENT_DATE() - 1, MONTH)
  AND segmentation = '1+'
  AND region = 'RegionA'
  GROUP BY a.date
  ORDER BY a.date
) d
LEFT JOIN (
  SELECT date, mau, mau / total AS ratio
  FROM (SELECT a.date, COUNT(DISTINCT b.user_id) AS mau
        FROM (
          SELECT DISTINCT date
          FROM project_id.analytics.user_actions
          WHERE date BETWEEN (
              DATE_TRUNC( CASE WHEN EXTRACT(DAY FROM LAST_DAY(DATE_SUB(CURRENT_DATE() - 1, INTERVAL 1 MONTH))) >=
              EXTRACT(DAY FROM LAST_DAY(CURRENT_DATE() - 1))
              THEN DATE_SUB(CURRENT_DATE() - 1, INTERVAL 1 MONTH)
              ELSE DATE_SUB(CURRENT_DATE() - 1, INTERVAL 2 MONTH)
              END, MONTH )
            ) AND (
              LAST_DAY(
              CASE WHEN EXTRACT(DAY FROM LAST_DAY(DATE_SUB(CURRENT_DATE() - 1, INTERVAL 1 MONTH))) >=
              EXTRACT(DAY FROM LAST_DAY(CURRENT_DATE() - 1))
              THEN DATE_SUB(CURRENT_DATE() - 1, INTERVAL 1 MONTH)
              ELSE DATE_SUB(CURRENT_DATE() - 1, INTERVAL 2 MONTH)
              END)
            )
        ) a
        CROSS JOIN project_id.analytics.user_actions b
        WHERE b.date <= a.date
        AND b.date >= DATE_TRUNC(CASE
          WHEN EXTRACT(DAY FROM LAST_DAY(DATE_SUB(CURRENT_DATE() - 1, INTERVAL 1 MONTH))) >=
          EXTRACT(DAY FROM LAST_DAY(CURRENT_DATE() - 1))
          THEN DATE_SUB(CURRENT_DATE() - 1, INTERVAL 1 MONTH)
          ELSE DATE_SUB(CURRENT_DATE() - 1, INTERVAL 2 MONTH)
          END, MONTH
        )
        AND segmentation = '1+'
        AND region = 'RegionA'
        GROUP BY a.date
        ORDER BY a.date
       )
  CROSS JOIN (
    SELECT COUNT(DISTINCT user_id) AS total
    FROM project_id.analytics.user_actions
    WHERE date BETWEEN (
        DATE_TRUNC( CASE WHEN EXTRACT(DAY FROM LAST_DAY(DATE_SUB(CURRENT_DATE() - 1, INTERVAL 1 MONTH))) >=
        EXTRACT(DAY FROM LAST_DAY(CURRENT_DATE() - 1))
        THEN DATE_SUB(CURRENT_DATE() - 1, INTERVAL 1 MONTH)
        ELSE DATE_SUB(CURRENT_DATE() - 1, INTERVAL 2 MONTH)
        END, MONTH )
      ) AND (
        LAST_DAY(
        CASE WHEN EXTRACT(DAY FROM LAST_DAY(DATE_SUB(CURRENT_DATE() - 1, INTERVAL 1 MONTH))) >=
        EXTRACT(DAY FROM LAST_DAY(CURRENT_DATE() - 1))
        THEN DATE_SUB(CURRENT_DATE() - 1, INTERVAL 1 MONTH)
        ELSE DATE_SUB(CURRENT_DATE() - 1, INTERVAL 2 MONTH)
        END)
      ) AND segmentation = '1+' AND region = 'RegionA'
  )
  ORDER BY 1
) rat ON EXTRACT(DAY FROM rat.date) = EXTRACT(DAY FROM d.date)
ORDER BY date;
--mau RegionA 60+
TRUNCATE TABLE project_id.analytics.mau_l_60;
INSERT INTO project_id.analytics.mau_l_60
SELECT d.date, d.mau AS fact_mau, p_l_60 AS planned_mau, EXTRACT(DAY FROM d.date) AS day_number, CASE WHEN ratio IS NOT NULL THEN ROUND(d.mau / ratio) END AS forecast_mau
FROM (
  SELECT a.date, COUNT(DISTINCT b.user_id) AS mau
  FROM (SELECT DISTINCT date
        FROM project_id.analytics.user_actions
        WHERE EXTRACT(YEAR FROM date) = EXTRACT(YEAR FROM CURRENT_DATE() - 1) AND EXTRACT(MONTH FROM date) = EXTRACT(MONTH FROM CURRENT_DATE() - 1)
       ) a
  CROSS JOIN project_id.analytics.user_actions b
  WHERE b.date <= a.date
  AND b.date >= DATE_TRUNC(CURRENT_DATE() - 1, MONTH)
  AND segmentation = '60+'
  AND region = 'RegionA'
  GROUP BY a.date
  ORDER BY a.date
) d
LEFT JOIN (
  SELECT date, mau, mau / total AS ratio
  FROM (SELECT a.date, COUNT(DISTINCT b.user_id) AS mau
        FROM (
          SELECT DISTINCT date
          FROM project_id.analytics.user_actions
          WHERE date BETWEEN (
              DATE_TRUNC( CASE WHEN EXTRACT(DAY FROM LAST_DAY(DATE_SUB(CURRENT_DATE() - 1, INTERVAL 1 MONTH))) >=
              EXTRACT(DAY FROM LAST_DAY(CURRENT_DATE() - 1))
              THEN DATE_SUB(CURRENT_DATE() - 1, INTERVAL 1 MONTH)
              ELSE DATE_SUB(CURRENT_DATE() - 1, INTERVAL 2 MONTH)
              END, MONTH )
            ) AND (
              LAST_DAY(
              CASE WHEN EXTRACT(DAY FROM LAST_DAY(DATE_SUB(CURRENT_DATE() - 1, INTERVAL 1 MONTH))) >=
              EXTRACT(DAY FROM LAST_DAY(CURRENT_DATE() - 1))
              THEN DATE_SUB(CURRENT_DATE() - 1, INTERVAL 1 MONTH)
              ELSE DATE_SUB(CURRENT_DATE() - 1, INTERVAL 2 MONTH)
              END)
            )
        ) a
        CROSS JOIN project_id.analytics.user_actions b
        WHERE b.date <= a.date
        AND b.date >= DATE_TRUNC(CASE
          WHEN EXTRACT(DAY FROM LAST_DAY(DATE_SUB(CURRENT_DATE() - 1, INTERVAL 1 MONTH))) >=
          EXTRACT(DAY FROM LAST_DAY(CURRENT_DATE() - 1))
          THEN DATE_SUB(CURRENT_DATE() - 1, INTERVAL 1 MONTH)
          ELSE DATE_SUB(CURRENT_DATE() - 1, INTERVAL 2 MONTH)
          END, MONTH
        )
        AND segmentation = '60+'
        AND region = 'RegionA'
        GROUP BY a.date
        ORDER BY a.date
       )
  CROSS JOIN (
    SELECT COUNT(DISTINCT user_id) AS total
    FROM project_id.analytics.user_actions
    WHERE date BETWEEN (
        DATE_TRUNC( CASE WHEN EXTRACT(DAY FROM LAST_DAY(DATE_SUB(CURRENT_DATE() - 1, INTERVAL 1 MONTH))) >=
        EXTRACT(DAY FROM LAST_DAY(CURRENT_DATE() - 1))
        THEN DATE_SUB(CURRENT_DATE() - 1, INTERVAL 1 MONTH)
        ELSE DATE_SUB(CURRENT_DATE() - 1, INTERVAL 2 MONTH)
        END, MONTH )
      ) AND (
        LAST_DAY(
        CASE WHEN EXTRACT(DAY FROM LAST_DAY(DATE_SUB(CURRENT_DATE() - 1, INTERVAL 1 MONTH))) >=
        EXTRACT(DAY FROM LAST_DAY(CURRENT_DATE() - 1))
        THEN DATE_SUB(CURRENT_DATE() - 1, INTERVAL 1 MONTH)
        ELSE DATE_SUB(CURRENT_DATE() - 1, INTERVAL 2 MONTH)
        END)
      ) AND segmentation = '60+' AND region = 'RegionA'
  )
  ORDER BY 1
) rat ON EXTRACT(DAY FROM rat.date) = EXTRACT(DAY FROM d.date)
ORDER BY date;
---mau 1+ RegionB
TRUNCATE TABLE project_id.analytics.mau_n;
INSERT INTO project_id.analytics.mau_n
SELECT d.date, d.mau AS fact_mau, p_n_1 AS planned_mau, EXTRACT(DAY FROM d.date) AS day_number, CASE WHEN ratio IS NOT NULL THEN ROUND(d.mau / ratio) END AS forecast_mau
FROM (
  SELECT a.date, COUNT(DISTINCT b.user_id) AS mau
  FROM (SELECT DISTINCT date
        FROM project_id.analytics.user_actions
        WHERE EXTRACT(YEAR FROM date) = EXTRACT(YEAR FROM CURRENT_DATE() - 1) AND EXTRACT(MONTH FROM date) = EXTRACT(MONTH FROM CURRENT_DATE() - 1)
       ) a
  CROSS JOIN project_id.analytics.user_actions b
  WHERE b.date <= a.date
  AND b.date >= DATE_TRUNC(CURRENT_DATE() - 1, MONTH)
  AND segmentation = '1+'
  AND region = 'RegionB'
  GROUP BY a.date
  ORDER BY a.date
) d
LEFT JOIN (
  SELECT date, mau, mau / total AS ratio
  FROM (SELECT a.date, COUNT(DISTINCT b.user_id) AS mau
        FROM (
          SELECT DISTINCT date
          FROM project_id.analytics.user_actions
          WHERE date BETWEEN (
              DATE_TRUNC( CASE WHEN EXTRACT(DAY FROM LAST_DAY(DATE_SUB(CURRENT_DATE() - 1, INTERVAL 1 MONTH))) >=
              EXTRACT(DAY FROM LAST_DAY(CURRENT_DATE() - 1))
              THEN DATE_SUB(CURRENT_DATE() - 1, INTERVAL 1 MONTH)
              ELSE DATE_SUB(CURRENT_DATE() - 1, INTERVAL 2 MONTH)
              END, MONTH )
            ) AND (
              LAST_DAY(
              CASE WHEN EXTRACT(DAY FROM LAST_DAY(DATE_SUB(CURRENT_DATE() - 1, INTERVAL 1 MONTH))) >=
              EXTRACT(DAY FROM LAST_DAY(CURRENT_DATE() - 1))
              THEN DATE_SUB(CURRENT_DATE() - 1, INTERVAL 1 MONTH)
              ELSE DATE_SUB(CURRENT_DATE() - 1, INTERVAL 2 MONTH)
              END)
            )
        ) a
        CROSS JOIN project_id.analytics.user_actions b
        WHERE b.date <= a.date
        AND b.date >= DATE_TRUNC(CASE
          WHEN EXTRACT(DAY FROM LAST_DAY(DATE_SUB(CURRENT_DATE() - 1, INTERVAL 1 MONTH))) >=
          EXTRACT(DAY FROM LAST_DAY(CURRENT_DATE() - 1))
          THEN DATE_SUB(CURRENT_DATE() - 1, INTERVAL 1 MONTH)
          ELSE DATE_SUB(CURRENT_DATE() - 1, INTERVAL 2 MONTH)
          END, MONTH
        )
        AND segmentation = '1+'
        AND region = 'RegionB'
        GROUP BY a.date
        ORDER BY a.date
       )
  CROSS JOIN (
    SELECT COUNT(DISTINCT user_id) AS total
    FROM project_id.analytics.user_actions
    WHERE date BETWEEN (
        DATE_TRUNC( CASE WHEN EXTRACT(DAY FROM LAST_DAY(DATE_SUB(CURRENT_DATE() - 1, INTERVAL 1 MONTH))) >=
        EXTRACT(DAY FROM LAST_DAY(CURRENT_DATE() - 1))
        THEN DATE_SUB(CURRENT_DATE() - 1, INTERVAL 1 MONTH)
        ELSE DATE_SUB(CURRENT_DATE() - 1, INTERVAL 2 MONTH)
        END, MONTH )
      ) AND (
        LAST_DAY(
        CASE WHEN EXTRACT(DAY FROM LAST_DAY(DATE_SUB(CURRENT_DATE() - 1, INTERVAL 1 MONTH))) >=
        EXTRACT(DAY FROM LAST_DAY(CURRENT_DATE() - 1))
        THEN DATE_SUB(CURRENT_DATE() - 1, INTERVAL 1 MONTH)
        ELSE DATE_SUB(CURRENT_DATE() - 1, INTERVAL 2 MONTH)
        END)
      ) AND segmentation = '1+' AND region = 'RegionB'
  )
  ORDER BY 1
) rat ON EXTRACT(DAY FROM rat.date) = EXTRACT(DAY FROM d.date)
ORDER BY date;
--mau RegionB 60+
TRUNCATE TABLE project_id.analytics.mau_n_60;
INSERT INTO project_id.analytics.mau_n_60
SELECT d.date, d.mau AS fact_mau, p_n_60 AS planned_mau, EXTRACT(DAY FROM d.date) AS day_number, CASE WHEN ratio IS NOT NULL THEN ROUND(d.mau / ratio) END AS forecast_mau
FROM (
  SELECT a.date, COUNT(DISTINCT b.user_id) AS mau
  FROM (SELECT DISTINCT date
        FROM project_id.analytics.user_actions
        WHERE EXTRACT(YEAR FROM date) = EXTRACT(YEAR FROM CURRENT_DATE() - 1) AND EXTRACT(MONTH FROM date) = EXTRACT(MONTH FROM CURRENT_DATE() - 1)
       ) a
  CROSS JOIN project_id.analytics.user_actions b
  WHERE b.date <= a.date
  AND b.date >= DATE_TRUNC(CURRENT_DATE() - 1, MONTH)
  AND segmentation = '60+'
  AND region = 'RegionB'
  GROUP BY a.date
  ORDER BY a.date
) d
LEFT JOIN (
  SELECT date, mau, mau / total AS ratio
  FROM (SELECT a.date, COUNT(DISTINCT b.user_id) AS mau
        FROM (
          SELECT DISTINCT date
          FROM project_id.analytics.user_actions
          WHERE date BETWEEN (
              DATE_TRUNC( CASE WHEN EXTRACT(DAY FROM LAST_DAY(DATE_SUB(CURRENT_DATE() - 1, INTERVAL 1 MONTH))) >=
              EXTRACT(DAY FROM LAST_DAY(CURRENT_DATE() - 1))
              THEN DATE_SUB(CURRENT_DATE() - 1, INTERVAL 1 MONTH)
              ELSE DATE_SUB(CURRENT_DATE() - 1, INTERVAL 2 MONTH)
              END, MONTH )
            ) AND (
              LAST_DAY(
              CASE WHEN EXTRACT(DAY FROM LAST_DAY(DATE_SUB(CURRENT_DATE() - 1, INTERVAL 1 MONTH))) >=
              EXTRACT(DAY FROM LAST_DAY(CURRENT_DATE() - 1))
              THEN DATE_SUB(CURRENT_DATE() - 1, INTERVAL 1 MONTH)
              ELSE DATE_SUB(CURRENT_DATE() - 1, INTERVAL 2 MONTH)
              END)
            )
        ) a
        CROSS JOIN project_id.analytics.user_actions b
        WHERE b.date <= a.date
        AND b.date >= DATE_TRUNC(CASE
          WHEN EXTRACT(DAY FROM LAST_DAY(DATE_SUB(CURRENT_DATE() - 1, INTERVAL 1 MONTH))) >=
          EXTRACT(DAY FROM LAST_DAY(CURRENT_DATE() - 1))
          THEN DATE_SUB(CURRENT_DATE() - 1, INTERVAL 1 MONTH)
          ELSE DATE_SUB(CURRENT_DATE() - 1, INTERVAL 2 MONTH)
          END, MONTH
        )
        AND segmentation = '60+'
        AND region = 'RegionB'
        GROUP BY a.date
        ORDER BY a.date
       )
  CROSS JOIN (
    SELECT COUNT(DISTINCT user_id) AS total
    FROM project_id.analytics.user_actions
    WHERE date BETWEEN (
        DATE_TRUNC( CASE WHEN EXTRACT(DAY FROM LAST_DAY(DATE_SUB(CURRENT_DATE() - 1, INTERVAL 1 MONTH))) >=
        EXTRACT(DAY FROM LAST_DAY(CURRENT_DATE() - 1))
        THEN DATE_SUB(CURRENT_DATE() - 1, INTERVAL 1 MONTH)
        ELSE DATE_SUB(CURRENT_DATE() - 1, INTERVAL 2 MONTH)
        END, MONTH )
      ) AND (
        LAST_DAY(
        CASE WHEN EXTRACT(DAY FROM LAST_DAY(DATE_SUB(CURRENT_DATE() - 1, INTERVAL 1 MONTH))) >=
        EXTRACT(DAY FROM LAST_DAY(CURRENT_DATE() - 1))
        THEN DATE_SUB(CURRENT_DATE() - 1, INTERVAL 1 MONTH)
        ELSE DATE_SUB(CURRENT_DATE() - 1, INTERVAL 2 MONTH)
        END)
      ) AND segmentation = '60+' AND region = 'RegionB'
  )
  ORDER BY 1
) rat ON EXTRACT(DAY FROM rat.date) = EXTRACT(DAY FROM d.date)
ORDER BY date;
DELETE FROM `project_id.analytics.mau_country` WHERE month = DATE_TRUNC(CURRENT_DATE() - 1, MONTH);
INSERT INTO `project_id.analytics.mau_country`
SELECT DATE_TRUNC(date, MONTH) AS month, CASE WHEN country IN ('Country1', 'Country2', 'Country3', 'Country4', 'Country5', 'Country6', 'Country7', 'Country8', 'Country9', 'Country10',
'Country11', 'Country12', 'Country13', 'Country14',
'Country15', 'Country16', 'Country17', 'Country18',
'Country19', 'Country20', 'Country21', 'Country22',
'Country23', 'Country24', 'Country25', 'Country26', 'Country27', 'Country28', 'Country29', 'Country30') THEN 'RegionA' ELSE 'RegionB' END AS region, COUNT(DISTINCT user_id) AS mau
FROM project_id.analytics.user_actions
WHERE DATE_TRUNC(date, MONTH) = DATE_TRUNC(CURRENT_DATE() - 1, MONTH) AND segmentation = '1+'
GROUP BY 1, 2
ORDER BY 1, 2;
DELETE FROM `project_id.analytics.mau_60_country` WHERE month = DATE_TRUNC(CURRENT_DATE() - 1, MONTH);
INSERT INTO `project_id.analytics.mau_60_country`
SELECT DATE_TRUNC(date, MONTH) AS month, CASE WHEN country IN ('Country1', 'Country2', 'Country3', 'Country4', 'Country5', 'Country6', 'Country7', 'Country8', 'Country9', 'Country10',
'Country11', 'Country12', 'Country13', 'Country14',
'Country15', 'Country16', 'Country17', 'Country18',
'Country19', 'Country20', 'Country21', 'Country22',
'Country23', 'Country24', 'Country25', 'Country26', 'Country27', 'Country28', 'Country29', 'Country30') THEN 'RegionA' ELSE 'RegionB' END AS region, COUNT(DISTINCT user_id) AS mau
FROM project_id.analytics.user_actions
WHERE DATE_TRUNC(date, MONTH) = DATE_TRUNC(CURRENT_DATE() - 1, MONTH) AND segmentation = '60+'
GROUP BY 1, 2
ORDER BY 1, 2;
DELETE FROM `project_id.analytics.mau_country_1` WHERE month = DATE_TRUNC(CURRENT_DATE() - 1, MONTH);
INSERT INTO `project_id.analytics.mau_country_1`
SELECT DATE_TRUNC(date, MONTH) AS month, country, CASE WHEN country IN ('Country1', 'Country2', 'Country3', 'Country4', 'Country5', 'Country6', 'Country7', 'Country8', 'Country9', 'Country10',
'Country11', 'Country12', 'Country13', 'Country14',
'Country15', 'Country16', 'Country17', 'Country18',
'Country19', 'Country20', 'Country21', 'Country22',
'Country23', 'Country24', 'Country25', 'Country26', 'Country27', 'Country28', 'Country29', 'Country30') THEN 'RegionA' ELSE 'RegionB' END AS region_1, COUNT(DISTINCT user_id) AS mau
FROM project_id.analytics.user_actions
WHERE DATE_TRUNC(date, MONTH) = DATE_TRUNC(CURRENT_DATE() - 1, MONTH) AND segmentation = '1+'
GROUP BY 1, 2
ORDER BY 1, 2;
DELETE FROM `project_id.analytics.mau_60_country_1` WHERE month = DATE_TRUNC(CURRENT_DATE() - 1, MONTH);
INSERT INTO `project_id.analytics.mau_60_country_1`
SELECT DATE_TRUNC(date, MONTH) AS month, country, CASE WHEN country IN ('Country1', 'Country2', 'Country3', 'Country4', 'Country5', 'Country6', 'Country7', 'Country8', 'Country9', 'Country10',
'Country11', 'Country12', 'Country13', 'Country14',
'Country15', 'Country16', 'Country17', 'Country18',
'Country19', 'Country20', 'Country21', 'Country22',
'Country23', 'Country24', 'Country25', 'Country26', 'Country27', 'Country28', 'Country29', 'Country30') THEN 'RegionA' ELSE 'RegionB' END AS region_1, COUNT(DISTINCT user_id) AS mau
FROM project_id.analytics.user_actions
WHERE DATE_TRUNC(date, MONTH) = DATE_TRUNC(CURRENT_DATE() - 1, MONTH) AND segmentation = '60+'
GROUP BY 1, 2
ORDER BY 1, 2;
----table for users lack
DELETE FROM project_id.user_lack.base_data WHERE DATE(created_at) = CURRENT_DATE() - 1;
INSERT INTO project_id.user_lack.base_data
SELECT DATE(created_at) AS date, CASE WHEN EXTRACT(HOUR FROM created_at) BETWEEN 0 AND 5 THEN 'night'
WHEN EXTRACT(HOUR FROM created_at) BETWEEN 6 AND 11 THEN 'morning'
WHEN EXTRACT(HOUR FROM created_at) BETWEEN 12 AND 17 THEN 'day'
WHEN EXTRACT(HOUR FROM created_at) BETWEEN 18 AND 23 THEN 'evening' END AS type_day, EXTRACT(HOUR FROM created_at) AS hour, CASE WHEN cont.region = 'RegionX' THEN 'RegionX' ELSE continent END AS country, vp.user_id, vp.to_user_id, vp.created_at, vp.duration
FROM project_id.analytics.user_actions act
LEFT JOIN project_id.mysql.activity_log vp ON vp.to_user_id = act.user_id AND DATE(vp.created_at) = act.date
LEFT JOIN project_id.mysql.countries_continents cont ON cont.country = act.country
WHERE segmentation = '1+' AND CASE WHEN cont.region = 'RegionX' THEN 'RegionX' ELSE continent END IS NOT NULL AND DATE(created_at) = CURRENT_DATE() - 1
ORDER BY 1 DESC, 3 ASC;
---Top Groups page
--comparison top groups (only all segmentation)
---this week previous week
---next charts we sum instead of count_distinct since they take a lot of memory
TRUNCATE TABLE project_id.analytics.comparison_users_num_actions;
INSERT INTO project_id.analytics.comparison_users_num_actions
SELECT date, group_id, week, active_users
FROM (
  SELECT date, group_id, week, COUNT(DISTINCT user_id) AS active_users
  FROM (
    SELECT date, group_id, user_id, 'This week' AS week
    FROM project_id.analytics.user_actions
    WHERE date BETWEEN DATE_SUB(DATE_TRUNC(CURRENT_DATE() - 1, WEEK(MONDAY)), INTERVAL 1 WEEK) AND DATE_SUB(DATE_TRUNC(CURRENT_DATE() - 1, WEEK(MONDAY)), INTERVAL 1 DAY) AND segmentation = 'All'
    GROUP BY ALL
  )
  GROUP BY 1, 2, 3
  ORDER BY 1, 2, 3
)
UNION ALL
SELECT DATE_ADD(date, INTERVAL 7 DAY) AS date, group_id, week, active_users
FROM (
  SELECT date, group_id, week, COUNT(DISTINCT user_id) AS active_users
  FROM (
    SELECT date, group_id, user_id, 'Last week' AS week
    FROM project_id.analytics.user_actions
    WHERE date BETWEEN DATE_SUB(DATE_TRUNC(CURRENT_DATE() - 1, WEEK(MONDAY)), INTERVAL 2 WEEK) AND DATE_SUB(DATE_TRUNC(CURRENT_DATE() - 1, WEEK(MONDAY)), INTERVAL 1 WEEK) - 1 AND segmentation = 'All'
    GROUP BY ALL
  )
  GROUP BY 1, 2, 3
  ORDER BY 1, 2, 3
);
-- comparison_users_num_actions_wau (wau cells)
TRUNCATE TABLE `project_id.analytics.comparison_users_num_actions_wau`;
INSERT INTO `project_id.analytics.comparison_users_num_actions_wau`
SELECT group_id, week, active_users
FROM (
  SELECT group_id, week, COUNT(DISTINCT user_id) AS active_users
  FROM (
    SELECT group_id, user_id, 'This week' AS week
    FROM project_id.analytics.user_actions
    WHERE date BETWEEN DATE_SUB(DATE_TRUNC(CURRENT_DATE() - 1, WEEK(MONDAY)), INTERVAL 1 WEEK) AND DATE_SUB(DATE_TRUNC(CURRENT_DATE() - 1, WEEK(MONDAY)), INTERVAL 1 DAY) AND segmentation = 'All'
    GROUP BY ALL
  )
  GROUP BY 1, 2
  ORDER BY 1, 2
)
UNION ALL
SELECT group_id, week, active_users
FROM (
  SELECT group_id, week, COUNT(DISTINCT user_id) AS active_users
  FROM (
    SELECT group_id, user_id, 'Last week' AS week
    FROM project_id.analytics.user_actions
    WHERE date BETWEEN DATE_SUB(DATE_TRUNC(CURRENT_DATE() - 1, WEEK(MONDAY)), INTERVAL 2 WEEK) AND DATE_SUB(DATE_TRUNC(CURRENT_DATE() - 1, WEEK(MONDAY)), INTERVAL 1 WEEK) - 1 AND segmentation = 'All'
    GROUP BY ALL
  )
  GROUP BY 1, 2
  ORDER BY 1, 2
);
--comparison by platform
--next chart which compares by platforms
TRUNCATE TABLE project_id.analytics.comparison_new_users;
INSERT INTO project_id.analytics.comparison_new_users
SELECT a.date, a.group_id, h.user_id, 'Previous Week' AS filter, platform
FROM project_id.analytics.groups_for_top a
JOIN project_id.analytics.user_actions h ON h.user_id = a.user_id AND h.segmentation = 'All' AND DATE(h.date) BETWEEN DATE(a.date)
AND DATE(DATE_TRUNC(DATE(a.date), WEEK(MONDAY)) + INTERVAL 6 DAY)
LEFT JOIN project_id.mysql.users_locale u ON u.id = a.user_id
WHERE event = 'Activated' AND a.date BETWEEN DATE_SUB(DATE_TRUNC(CURRENT_DATE() - 1, WEEK(MONDAY)), INTERVAL 2 WEEK) AND DATE(DATE_SUB(DATE_TRUNC(CURRENT_DATE() - 1, WEEK(MONDAY)), INTERVAL 1 WEEK) - INTERVAL 1 DAY)
GROUP BY ALL
UNION ALL
SELECT a.date, a.group_id, h.user_id, 'This Week' AS filter, platform
FROM project_id.analytics.groups_for_top a
JOIN project_id.analytics.user_actions h ON h.user_id = a.user_id AND h.segmentation = 'All' AND DATE(h.date) BETWEEN DATE(a.date)
AND DATE(DATE_TRUNC(DATE(a.date), WEEK(MONDAY)) + INTERVAL 6 DAY)
LEFT JOIN project_id.mysql.users_locale u ON u.id = a.user_id
WHERE event = 'Activated' AND a.date BETWEEN DATE_SUB(DATE_TRUNC(CURRENT_DATE() - 1, WEEK(MONDAY)), INTERVAL 1 WEEK) AND DATE_SUB(DATE_TRUNC(CURRENT_DATE() - 1, WEEK(MONDAY)), INTERVAL 1 DAY)
GROUP BY ALL;
--ratio new to active users
TRUNCATE TABLE project_id.analytics.top_new_users;
INSERT INTO project_id.analytics.top_new_users
SELECT a.date, a.group_id, a.user_id, h.user_id AS segmented_user_id, 'Previous Week' AS filter
FROM project_id.analytics.groups_for_top a
LEFT JOIN project_id.analytics.user_actions h ON h.user_id = a.user_id AND h.segmentation = 'All' AND DATE(h.date) BETWEEN DATE(a.date)
AND DATE(DATE_TRUNC(DATE(a.date), WEEK(MONDAY)) + INTERVAL 6 DAY)
WHERE event = 'Registrations' AND a.date BETWEEN DATE_SUB(DATE_TRUNC(CURRENT_DATE() - 1, WEEK(MONDAY)), INTERVAL 2 WEEK) AND DATE(DATE_SUB(DATE_TRUNC(CURRENT_DATE() - 1, WEEK(MONDAY)), INTERVAL 1 WEEK) - INTERVAL 1 DAY)
GROUP BY ALL
UNION ALL
SELECT a.date, a.group_id, a.user_id, h.user_id AS segmented_user_id, 'This Week' AS filter
FROM project_id.analytics.groups_for_top a
LEFT JOIN project_id.analytics.user_actions h ON h.user_id = a.user_id AND h.segmentation = 'All' AND DATE(h.date) BETWEEN DATE(a.date)
AND DATE(DATE_TRUNC(DATE(a.date), WEEK(MONDAY)) + INTERVAL 6 DAY)
WHERE event = 'Registrations' AND a.date BETWEEN DATE_SUB(DATE_TRUNC(CURRENT_DATE() - 1, WEEK(MONDAY)), INTERVAL 1 WEEK) AND DATE_SUB(DATE_TRUNC(CURRENT_DATE() - 1, WEEK(MONDAY)), INTERVAL 1 DAY)
GROUP BY ALL;
-- comparison new users country (pie charts, country)
TRUNCATE TABLE project_id.analytics.comparison_new_users_country;
INSERT INTO project_id.analytics.comparison_new_users_country
SELECT date, group_id, country, week, active_users
FROM (
  SELECT date, group_id, country, week, COUNT(DISTINCT user_id) AS active_users
  FROM (
    SELECT date, group_id, country, user_id, 'This week' AS week
    FROM project_id.analytics.user_actions
    WHERE date BETWEEN DATE_SUB(DATE_TRUNC(CURRENT_DATE() - 1, WEEK(MONDAY)), INTERVAL 1 WEEK) AND DATE_SUB(DATE_TRUNC(CURRENT_DATE() - 1, WEEK(MONDAY)), INTERVAL 1 DAY) AND segmentation = 'All'
    GROUP BY ALL
  )
  GROUP BY 1, 2, 3, 4
  ORDER BY 1, 2, 3, 4
)
UNION ALL
SELECT date, group_id, country, week, active_users
FROM (
  SELECT date, group_id, country, week, COUNT(DISTINCT user_id) AS active_users
  FROM (
    SELECT date, group_id, country, user_id, 'Last week' AS week
    FROM project_id.analytics.user_actions
    WHERE date BETWEEN DATE_SUB(DATE_TRUNC(CURRENT_DATE() - 1, WEEK(MONDAY)), INTERVAL 2 WEEK) AND DATE_SUB(DATE_TRUNC(CURRENT_DATE() - 1, WEEK(MONDAY)), INTERVAL 1 WEEK) - 1 AND segmentation = 'All'
    GROUP BY ALL
  )
  GROUP BY 1, 2, 3, 4
  ORDER BY 1, 2, 3, 4
);
--hourly dau
TRUNCATE TABLE project_id.analytics.hourly_dau;
INSERT INTO project_id.analytics.hourly_dau
(SELECT DATE(vp.created_at) AS date, EXTRACT(HOUR FROM vp.created_at) AS hour, group_id, to_user_id, 'UTC' AS local_time
FROM project_id.mysql.activity_log vp
LEFT JOIN project_id.analytics.user_actions num ON num.user_id = vp.to_user_id AND num.segmentation = 'All' AND num.date = DATE(vp.created_at)
WHERE vp.duration > 0 AND
TIMESTAMP(vp.created_at) BETWEEN TIMESTAMP(DATETIME_SUB(DATETIME_TRUNC(DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 1 DAY), WEEK(MONDAY)), INTERVAL 1 WEEK)) AND TIMESTAMP(DATETIME_SUB(DATETIME_TRUNC(DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 1 DAY), WEEK(MONDAY)), INTERVAL 1 SECOND)) AND num.user_id IS NOT NULL
GROUP BY ALL)
UNION ALL
--local time for hour
(SELECT DATE(TIMESTAMP(DATETIME(created_at), timezone)) AS date, EXTRACT(HOUR FROM TIMESTAMP(DATETIME(created_at), timezone)) AS hour, num.group_id, to_user_id, 'Local Hour' AS local_time
FROM project_id.mysql.activity_log vp
LEFT JOIN project_id.analytics.user_actions num ON num.user_id = vp.to_user_id AND num.segmentation = 'All' AND num.date = DATE(vp.created_at)
LEFT JOIN project_id.analytics.groups_timezones t ON (CASE WHEN t.group_id IS NULL THEN 0 ELSE t.group_id END) = (CASE WHEN num.group_id IS NULL THEN 0 ELSE num.group_id END)
WHERE vp.duration > 0 AND
TIMESTAMP(DATETIME(vp.created_at), timezone) BETWEEN TIMESTAMP(DATETIME_SUB(DATETIME_TRUNC(DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 1 DAY), WEEK(MONDAY)), INTERVAL 1 WEEK), timezone) AND TIMESTAMP(DATETIME_SUB(DATETIME_TRUNC(DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 1 DAY), WEEK(MONDAY)), INTERVAL 1 SECOND), timezone) AND num.user_id IS NOT NULL
GROUP BY ALL);
--top weeks points
TRUNCATE TABLE project_id.analytics.top_weeks_points;
INSERT INTO project_id.analytics.top_weeks_points
SELECT user_id, group_id, event_date, amount_points, 'Previous Week' AS filter
FROM (
  SELECT s.user_id, group_id, DATE(vp.created_at) AS event_date, ROUND(SUM(amount)) AS amount_points
  FROM project_id.mysql.user_role s
  LEFT JOIN project_id.mysql.user_ip_country ip ON s.user_id = ip.user_id
  LEFT JOIN project_id.mysql.user_balance_history vp ON s.user_id = vp.user_id
  WHERE DATE(vp.created_at) BETWEEN DATE_SUB(DATE_TRUNC(CURRENT_DATE() - 1, WEEK(MONDAY)), INTERVAL 2 WEEK) AND DATE(DATE_SUB(DATE_TRUNC(CURRENT_DATE() - 1, WEEK(MONDAY)), INTERVAL 1 WEEK) - INTERVAL 1 DAY) AND JSON_VALUE(description, '$.action') NOT IN ('withdraw_deduction')
  GROUP BY 1, 2, 3
)
UNION ALL
SELECT user_id, group_id, event_date, amount_points, 'This Week' AS filter
FROM (
  SELECT s.user_id, group_id, DATE(vp.created_at) AS event_date, ROUND(SUM(amount)) AS amount_points
  FROM project_id.mysql.user_role s
  LEFT JOIN project_id.mysql.user_ip_country ip ON s.user_id = ip.user_id
  LEFT JOIN project_id.mysql.user_balance_history vp ON s.user_id = vp.user_id
  WHERE DATE(vp.created_at) BETWEEN DATE_SUB(DATE_TRUNC(CURRENT_DATE() - 1, WEEK(MONDAY)), INTERVAL 1 WEEK) AND DATE_SUB(DATE_TRUNC(CURRENT_DATE() - 1, WEEK(MONDAY)), INTERVAL 1 DAY) AND JSON_VALUE(description, '$.action') NOT IN ('withdraw_deduction')
  GROUP BY 1, 2, 3
);
-- FOR REPORTING TOOL
--user_actions
TRUNCATE TABLE `project_id.analytics.user_actions_ls`;
INSERT INTO `project_id.analytics.user_actions_ls`
SELECT *, CASE WHEN week = DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 WEEK), WEEK(MONDAY)) THEN 'Last Week'
WHEN week = DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 2 WEEK), WEEK(MONDAY)) THEN 'Penultimate Week' END AS filter_week
FROM (
  SELECT group_id, country, region, a.date, segmentation, a.user_id, DATE_TRUNC(a.date, WEEK(MONDAY)) AS week, CASE WHEN platform = 2 THEN 'PlatformA'
  WHEN platform = 3 THEN 'PlatformB' WHEN platform = 1 THEN 'Web' END AS platform,
  CASE WHEN a.date = DATE(create_ts) THEN 'new' ELSE 'old' END AS type, CASE WHEN p.user_id IS NOT NULL THEN TRUE ELSE FALSE END AS public_visible
  FROM project_id.analytics.user_actions a
  LEFT JOIN project_id.mysql.users_locale u ON u.id = a.user_id
  LEFT JOIN project_id.analytics.public_visible p ON p.user_id = a.user_id AND p.date = a.date
  WHERE a.date BETWEEN CURRENT_DATE() - 92 AND CURRENT_DATE() - 1
);
-- average rating
TRUNCATE TABLE `project_id.analytics.average_rating_ls`;
INSERT INTO `project_id.analytics.average_rating_ls`
SELECT COUNT(DISTINCT user_id) AS user_id, country, continent, ROUND(AVG(rating), 2) AS rating, DATE(call_date) AS call_date FROM `project_id.analytics.average_rating` GROUP BY ALL;
TRUNCATE TABLE `project_id.analytics.average_rating_2_ls`;
INSERT INTO `project_id.analytics.average_rating_2_ls`
SELECT COUNT(DISTINCT user_id) AS user_id, country_2, continent, ROUND(AVG(rating), 2) AS rating, DATE(call_date) AS call_date FROM `project_id.analytics.average_rating_2` GROUP BY ALL;
-- country funnel
TRUNCATE TABLE `project_id.analytics.country_2_funnel_ls`;
INSERT INTO `project_id.analytics.country_2_funnel_ls`
SELECT date, COUNT(DISTINCT user_id) AS user_id, country_2, event
FROM `project_id.analytics.country_2_funnel` GROUP BY ALL;
TRUNCATE TABLE `project_id.analytics.country_funnel_ls`;
INSERT INTO `project_id.analytics.country_funnel_ls`
SELECT date, COUNT(DISTINCT user_id) AS user_id, country, event
FROM `project_id.analytics.country_funnel` GROUP BY ALL;
