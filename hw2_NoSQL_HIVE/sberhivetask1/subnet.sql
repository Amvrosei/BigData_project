ADD jar /opt/cloudera/parcels/CDH/lib/hive/lib/hive-serde.jar;

USE krasavinan;

DROP TABLE IF EXISTS Subnets;

CREATE EXTERNAL TABLE Subnets (
    ip STRING,
    mask STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY  '\t'
STORED AS TEXTFILE
LOCATION '/data/subnets/variant1';

select * from Subnets limit 10;
