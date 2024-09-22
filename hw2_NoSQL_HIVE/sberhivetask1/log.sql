ADD jar /opt/cloudera/parcels/CDH/lib/hive/lib/hive-serde.jar;

set hive.exec.max.dynamic.partitions=5000;
set hive.exec.max.dynamic.partitions.pernode=1000;

USE krasavinan;
--USE test_k_07;

DROP TABLE IF EXISTS Logs_help;

DROP TABLE IF EXISTS Logs_help;

CREATE EXTERNAL TABLE Logs_help (
        ip STRING,
        date_time INT,
        http_request STRING,
        size INT,
        http_status INT,
        browser STRING
) 
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
        "input.regex" = '^(\\S*)\\t\\t\\t(\\d{8})\\S*\\t(\\S*)\\t(\\d*)\\t(\\d*)\\t(\\S*?(?=\\s)).*$'
)
STORED AS TEXTFILE
LOCATION '/data/user_logs/user_logs_M';


SET hive.exec.dynamic.partition.mode=nonstrict;

USE krasavinan;
DROP TABLE IF EXISTS Logs;

SET hive.exec.dynamic.partition.mode=nonstrict;

DROP TABLE IF EXISTS Logs;

CREATE EXTERNAL TABLE logs (
    ip STRING,
    http_request STRING,
    size INT,
    http_status INT,
    browser STRING
)
PARTITIONED BY (date_time INT)
STORED AS TEXTFILE;

INSERT OVERWRITE TABLE Logs PARTITION (date_time)
SELECT ip ,
        http_request , 
        size , 
        http_status , 
        browser ,
        date_time 
FROM logs_help;

SELECT * FROM Logs limit 10;
