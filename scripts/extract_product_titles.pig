--Extract the productId and title for the search indexer.

set job.name 'isura_extract_product_titles';

reviews = load '$reviews' using PigStorage('\t') as (product_id:chararray, title:chararray, price:chararray, user_id:chararray, profile_name:chararray, helpfulness:chararray, score:int, time:long, summary:chararray, text:chararray);

reviews_clean = filter reviews by user_id != 'unknown' and title != '' and text != '';

products = foreach reviews_clean generate product_id, title;

products_grouped = group products by product_id;

product_titles = foreach products_grouped {
    singleton = limit products 1;
    generate flatten(singleton);
}

store product_titles into '$output' using PigStorage('\t');
