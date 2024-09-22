ADD jar /opt/cloudera/parcels/CDH/lib/hive/lib/hive-serde.jar;
SET hive.auto.convert.join = false;

USE krasavinan;

SELECT TRANSFORM(ip, date_time, http_request, size, http_status, browser)
USING "sed -r 's|.ru/|.com/|'" AS ip, date_time, http_request, size, http_status, browser
FROM Logs_help
LIMIT 10;
