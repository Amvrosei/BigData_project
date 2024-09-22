USE krasavinan;

SELECT date_time, COUNT(*) as cnt_ FROM Logs
GROUP BY date_time
ORDER BY cnt_ DESC;