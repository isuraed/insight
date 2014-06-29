-- In the Amazon reviews data identical review text occurs for different products
-- and same user. The review is probably propagated by Amazon to new products.
-- This script removes those duplicate reviews and arbitrarily chooses one.

register 'lib/datafu-1.2.0.jar';

define MD5 datafu.pig.hash.MD5();

set job.name 'isura_remove_duplicates';

reviews = load '$reviews' using PigStorage('\t') as (product_id:chararray, title:chararray, price:chararray, user_id:chararray, profile_name:chararray, helpfulness:chararray, score:int, time:long, summary:chararray, text:chararray);

reviews_clean = filter reviews by user_id != 'unknown'

-- Use MD5 because it's much faster to group by hash string than review text.
-- MD5 will crash if called on an empty string.
reviews_hashed = foreach reviews_clean generate product_id, title, price, user_id, profile_name, helpfulness, score, time, summary, text, (text is null ? '' : MD5(text)) as text_hash;

reviews_grouped = group reviews_hashed by (user_id, text_hash);

reviews_unique = foreach reviews_grouped {
    unique = limit reviews_hashed 1;
    generate flatten(unique);
};

-- We don't need the MD5 value. Output should have same schema as input.
reviews_output = foreach reviews_unique generate product_id, title, price, user_id, profile_name, helpfulness, score, time, summary, text;

store reviews_output into '$output' using PigStorage('\t');
