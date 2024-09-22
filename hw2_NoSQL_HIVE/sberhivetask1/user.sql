ADD jar /opt/cloudera/parcels/CDH/lib/hive/lib/hive-serde.jar;

USE krasavinan;

DROP TABLE IF EXISTS Users;

CREATE EXTERNAL TABLE Users (
        ip STRING,
        browser STRING,
        sex STRING,
        age STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'

WITH SERDEPROPERTIES (
        "input.regex" = '^(\\S*)\\t(\\S*)\\t(\\S*)\\t(\\d+).*$',
        "output.format.string" = "%1$s %2$s %3$s %4$s"
)
STORED AS TEXTFILE
LOCATION '/data/user_logs/user_data_M';

select * from Users limit 10;