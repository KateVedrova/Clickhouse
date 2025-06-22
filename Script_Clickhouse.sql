--Создание таблицы user_events
CREATE TABLE user_events 
(
    user_id UInt32,
    event_type String,
    points_spent UInt32,
    event_time DateTime
)
ENGINE = MergeTree()
ORDER BY (event_time, user_id)
TTL event_time + INTERVAL 30 DAY;

--Создание агрегированной таблицы
CREATE TABLE user_agg 
(
    event_date Date,
    event_type String,
    uniq_users AggregateFunction(uniq, UInt32),
    sum_points AggregateFunction(sum, UInt32),
    count_action AggregateFunction(count, UInt8)
)
ENGINE = AggregatingMergeTree()
ORDER BY (event_date, event_type)
TTL event_date + INTERVAL 180 DAY;

--Создание Materialized View
CREATE MATERIALIZED VIEW mv_user_agg
 TO user_agg
 AS
 SELECT
     toDate(event_time) AS event_date,
     event_type,
     uniqState(user_id) AS uniq_users,
     sumState(points_spent) AS sum_points,
     countState() AS count_action
 FROM user_events
 GROUP BY event_date, event_type;

--Вставка данных в таблицу user_events
INSERT INTO user_events VALUES
(1, 'login', 0, now() - INTERVAL 10 DAY),
(2, 'signup', 0, now() - INTERVAL 10 DAY),
(3, 'login', 0, now() - INTERVAL 10 DAY),
(1, 'login', 0, now() - INTERVAL 7 DAY),
(2, 'login', 0, now() - INTERVAL 7 DAY),
(3, 'purchase', 30, now() - INTERVAL 7 DAY),
(1, 'purchase', 50, now() - INTERVAL 5 DAY),
(2, 'logout', 0, now() - INTERVAL 5 DAY),
(4, 'login', 0, now() - INTERVAL 5 DAY),
(1, 'login', 0, now() - INTERVAL 3 DAY),
(3, 'purchase', 70, now() - INTERVAL 3 DAY),
(5, 'signup', 0, now() - INTERVAL 3 DAY),
(2, 'purchase', 20, now() - INTERVAL 1 DAY),
(4, 'logout', 0, now() - INTERVAL 1 DAY),
(5, 'login', 0, now() - INTERVAL 1 DAY),
(1, 'purchase', 25, now()),
(2, 'login', 0, now()),
(3, 'logout', 0, now()),
(6, 'signup', 0, now()),
(6, 'purchase', 100, now());

-- Создание запроса, показывающего Retention
WITH 
first_activity AS (
    SELECT
        user_id,
        min(toDate(event_time)) AS first_date
    FROM user_events
    GROUP BY user_id
),
returned_users AS (
    SELECT
        COUNT(DISTINCT u.user_id) AS returned_count
    FROM first_activity fa
    INNER JOIN user_events u 
        ON fa.user_id = u.user_id
        AND datediff('day', fa.first_date, toDate(u.event_time)) BETWEEN 1 AND 7
)
SELECT
    (SELECT COUNT(DISTINCT user_id) FROM first_activity) AS total_users_day_0,
    (SELECT returned_count FROM returned_users) AS returned_in_7_days,
    ROUND(
        ((SELECT returned_count FROM returned_users) * 100.0 / 
         NULLIF((SELECT COUNT(DISTINCT user_id) FROM first_activity), 0)),
        2
    ) AS retention_7d_percent;

--Создание запроса с группировой по быстрой аналитике по дням
SELECT 
    event_date,
    event_type,
    uniqMerge(uniq_users) AS unique_users,
    sumMerge(sum_points) AS total_spent,
    countMerge(count_action) AS total_actions
FROM user_agg
GROUP BY event_date, event_type
ORDER BY event_date;








