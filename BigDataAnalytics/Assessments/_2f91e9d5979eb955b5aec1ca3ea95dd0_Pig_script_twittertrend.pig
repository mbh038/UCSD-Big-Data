-- ******************************************
-- An example script using JSON loader
-- ******************************************

--You might use hdfs commands from the unix prompt (which you should know by now),
-- or you can use and fs command within pig, to set up input files
-- For example, from the grunt > prompt you could type:
--fs -copyFromLocal /home/cloudera/PigCode/Pig/Twitter_Test.json /user/cloudera/pigin/

twitter  = LOAD '/user/cloudera/pigin/Twitter_Test.json' USING JsonLoader('created_time:chararray, text:chararray, user_id:chararray, id:chararray, created_date:chararray');
twt_samp = SAMPLE twitter 0.1;

-- ******************************************
-- get word counts for 1 date
-- ******************************************

twt_d1             = FILTER twt_samp BY created_date MATCHES 'Fri Oct 05 2012';
twt_d1_msgflt      = FOREACH twt_d1 GENERATE FLATTEN(TOKENIZE(text)) as msg_words;
twt_d1_msgflt_grpd = GROUP twt_d1_msgflt BY msg_words;

--optionally enter the following:
--tmpg5 = LIMIT twt_d1_msgflt_grpd 5;
--DUMP tmpg5

twt_d1_wdcnts = FOREACH twt_d1_msgflt_grpd GENERATE group AS word, COUNT(twt_d1_msgflt.msg_words) AS word_cnt;

describe twt_d1_wdcnts

-- ******************************************
--now get all word counts, then join by word to get d1 cnt,all cnts, num days, etc...
-- ******************************************

twt_dall_msgflt      = FOREACH twt_samp GENERATE FLATTEN(TOKENIZE(text)) as dall_msg_words;
twt_dall_msgflt_grpd = GROUP twt_dall_msgflt BY dall_msg_words;
twt_dall_wdcnts      = FOREACH twt_dall_msgflt_grpd GENERATE group AS word,

COUNT (twt_dall_msgflt.dall_msg_words) AS word_cnt;

-- optional stuff, if you want to save intermediate results:
--STORE twt_dall_wdcnts into '/user/cloudera/pigoutnew/twt_dall_wdcnts.txt';
--fs -copyToLocal /user/cloudera/pigoutnew/twt_dall_wdcnts.txt /home/cloudera/PigCode/twt_dall_wdcnts.txt

-- ******************************************
-- first get total date counts
-- ******************************************

twt_dates        = FOREACH twt_samp GENERATE created_date;
twt_dates_grpd   = GROUP twt_dates ALL;
twt_dates_totcnt = FOREACH twt_dates_grpd {
            uniq_dates= DISTINCT twt_dates.created_date;
           GENERATE COUNT(uniq_dates) AS total;
            }

DUMP twt_dates_totcnt

-- ******************************************
-- now joining twt_dall_wdcnts twt_d1_wdcnts
-- ******************************************

twt_d1dall_wdcnts_jnd = JOIN twt_d1_wdcnts BY word, twt_dall_wdcnts BY word;
twt_d1dall_normed     = FOREACH twt_d1dall_wdcnts_jnd {
     obs_freq = (double) twt_d1_wdcnts::word_cnt;
     exp_freq = (double) twt_dall_wdcnts::word_cnt;
     lift = obs_freq-(exp_freq/(double) twt_dates_totcnt.total);
     GENERATE twt_d1_wdcnts::word, obs_freq, exp_freq, lift;
     };

-- Note: a ROUND_TO function is in pig r0.13, my cloudera vm has pig r0.12

-- If you need to, clear out hdfs directory and files either from unix or grunt, for example,
--fs -rm /user/cloudera/pigoutnew/twt_d1dall_normed/*
--fs -rmdir /user/cloudera/pigoutnew/twt_d1dall_normed

-- Save the data
STORE twt_d1dall_normed into '/user/cloudera/pigoutnew/twt_d1dall_normed';

-- Then copy out of HDFS to your local file system either by using the shell and fs commands within grunt:
--sh rm /home/cloudera/PigCode/twt_d1dall_normed/*
--sh rmdir /home/cloudera/PigCode/twt_d1dall_normed
--fs -copyToLocal /user/cloudera/pigoutnew/twt_d1dall_normed /home/cloudera/PigCode/twt_d1dall_normed

-- OR, from Unix
-- rm /home/cloudera/PigCode/twt_d1dall_normed/*
-- rmdir /home/cloudera/PigCode/twt_d1dall_normed
-- hdfs dfs -copyToLocal /user/cloudera/pigoutnew/twt_d1dall_normed

