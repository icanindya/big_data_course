MOVIES = LOAD 'hdfs://cshadoop1.utdallas.edu/Spring-2016-input/movies.dat' USING PigStorage(':') as (mid:int, title:chararray, genres:chararray);
RATINGS = LOAD 'hdfs://cshadoop1.utdallas.edu/Spring-2016-input/ratings.dat' USING PigStorage(':') as (uid:int, mid:int, rating:int, time:long);
GROUPED = COGROUP MOVIES BY mid, RATINGS BY mid;
TOP_FIVE = LIMIT GROUPED 5;
DUMP TOP_FIVE;



