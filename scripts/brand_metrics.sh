pig -f pig/brand_metrics.pig -param reviews=$1 -param brands=$2 -param output=$3

hive -f hive/brand_metrics.q -hiveconf path='/user/ec2-user/'$3

