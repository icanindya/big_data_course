MOVIES = LOAD 'hdfs://cshadoop1.utdallas.edu/Spring-2016-input/movies.dat' USING PigStorage(':') as (mid:int, title:chararray, genres:chararray);
RATINGS = LOAD 'hdfs://cshadoop1.utdallas.edu/Spring-2016-input/ratings.dat' USING PigStorage(':') as (uid:int, mid:int, rating:int, time:long);
USERS = LOAD 'hdfs://cshadoop1.utdallas.edu/Spring-2016-input/users.dat' USING PigStorage(':') as (uid:int, gender:chararray, age:int, occupation:int, zip:chararray);

DESIRED_MOVIES = FILTER MOVIES BY (genres matches '.*Action.*War.*') OR (genres matches '.*War.*Action.*');
MOVIES_RATINGS = JOIN DESIRED_MOVIES BY mid, RATINGS BY mid;
GROUPED_R = GROUP MOVIES_RATINGS BY $0;
AVG_RATINGS = FOREACH GROUPED_R GENERATE group as mid, AVG(MOVIES_RATINGS.rating) as avgRating;
GROUPED_AVG = GROUP AVG_RATINGS ALL;
MIN_AVG = FOREACH GROUPED_AVG GENERATE MIN(AVG_RATINGS.avgRating) as minRating;
WORST_MOVIES = JOIN  MIN_AVG BY minRating, AVG_RATINGS BY avgRating;
WORST_MIDS = FOREACH WORST_MOVIES GENERATE $1 as mid;
WORST_MID_RATINGS = JOIN WORST_MIDS BY mid, RATINGS BY mid;
DESIRED_USERS = FILTER USERS BY (gender == 'F') AND (age >= 20 AND age <= 35) AND STARTSWITH(zip, '1');
USERS_RATED = JOIN WORST_MID_RATINGS BY uid, DESIRED_USERS BY uid;
UIDS = FOREACH USERS_RATED GENERATE $1;
DUMP UIDS;


