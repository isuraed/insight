set job.name 'isura_clean_reviews';

reviews = load '$input' using PigStorage('\t') as (product_id:chararray, title:chararray, price:chararray, user_id:chararray, profile_name:chararray, helpfulness:chararray, score:int, time:long, summary:chararray, text:chararray);

reviews_clean = filter reviews by user_id != 'unknown' and product_id != '' and title != '' and text != '';

store reviews_clean into '$output' using PigStorage('\t');
