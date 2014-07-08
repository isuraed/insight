-- Find the most reviewed products in the data set.

set job.name 'isura_most_reviewed';

reviews_unique = load '$reviews' using PigStorage('\t') as (product_id:chararray, title:chararray, price:chararray, user_id:chararray, profile_name:chararray, helpfulness:chararray, score:int, time:long, summary:chararray, text:chararray);

product_titles = load '$titles' using PigStorage('\t') as (product_id:chararray, title:chararray);

reviews_grouped = group reviews_unique by product_id;

reviews_count = foreach reviews_grouped generate group as product_id, COUNT(reviews_unique) as count;

reviews_count_limit = filter reviews_count by count >= 1000;

most_reviewed = join reviews_count_limit by product_id, product_titles by product_id;

most_reviewed_flat = foreach most_reviewed generate product_titles::product_id as product_id, product_titles::title as title, reviews_count_limit::count as count;

store most_reviewed_flat into 'hbase://isura_most_reviewed' using org.apache.pig.backend.hadoop.hbase.HBaseStorage('cf1:title,cf1:count');
