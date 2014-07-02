set job.name 'isura_averages';

reviews = load 'isura/in/reviews/*.tab' using PigStorage('\t') as (product_id:chararray, title:chararray, price:chararray, user_id:chararray, profile_name:chararray, helpfulness:chararray, score:int, time:long, summary:chararray, text:chararray);

scores = foreach reviews generate product_id, user_id, score;
unknown_scores = filter scores by user_id == 'unknown';
grouped_unknown_scores = group unknown_scores all;
avg = foreach grouped_unknown_scores generate AVG(unknown_scores.score);
dump avg;


--store unknown_scores into 'isura/out/unknown_scores' using PigStorage('\t');
