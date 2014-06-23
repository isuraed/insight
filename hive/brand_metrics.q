drop table isura_brand_metrics_temp;
--Because it's an internal table this will also drop the corresponding HBase table.
drop table isura_brand_metrics;

--Temporary table for bulk loading HBase.
create external table isura_brand_metrics_temp (brand string, review_count int, average_score float)
row format delimited
fields terminated by '\t'
stored as textfile
location '${hiveconf:path}';


create table isura_brand_metrics(brand string, review_count int, average_score float)
stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties ("hbase.columns.mapping" = ":key,cf1:review_count,cf1:average_score")
tblproperties ("hbase.table.name" = "isura_brand_metrics");

insert overwrite table isura_brand_metrics select brand, review_count, average_score from isura_brand_metrics_temp;

drop table isura_brand_metrics_temp;
