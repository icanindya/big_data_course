REGISTER udf.jar;
MOVIES = LOAD 'hdfs://cshadoop1.utdallas.edu/Spring-2016-input/movies.dat' USING PigStorage(':') as (mid:int, title:chararray, genres:chararray);
FORMATTED = FOREACH MOVIES GENERATE title, FORMAT_GENRE(genres);
DUMP FORMATTED;

