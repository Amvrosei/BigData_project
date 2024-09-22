ADD jar /opt/cloudera/parcels/CDH/lib/hive/lib/hive-serde.jar;

USE krasavinan;

DROP TABLE IF EXISTS IPRegions;

CREATE EXTERNAL TABLE IPRegions (
        ip STRING,
        region STRING
)

ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'

WITH SERDEPROPERTIES (
        "input.regex" = '^(\\S*)\\t(\\S*).*$',
        "output.format.string" = "%1$s %2$s"
)
STORED AS TEXTFILE
LOCATION '/data/user_logs/ip_data_M';

select * from IPRegions limit 10;