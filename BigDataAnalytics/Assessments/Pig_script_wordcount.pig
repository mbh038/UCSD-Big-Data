--A word count Pig script , save this text file into a

-- file that ends in '.pig' like 'wordcount.pig'

-- (careful with new lines when cutting &

-- pasting, you might need to run the following at the unix prompt

-- [unix]> dos2unix infile outfile or first try dos2unix --help

--run as either pig -x local wordcount.pig OR

-- pig -x mapreduce wordcount.pig

-- Note: pig by default only saves log files if there is an error

-- You should notice this on the screen output



-- Thus, to save standard output from describe and dumps

-- run it as , for example

-- pig -x local > mylog_output

-- or run it interactively and cut&paste from the screen

--don't forget to set up input files in hdfs if you use mapreduce

-- e.g. hdfs dfs -mkdir /user/cloudera/pigin

-- hdfs dfs -copyFromLocal /home/cloudera/testfile* /user/cloudera/pigin

--don't forget to set up an output folder in hdfs if you use mapreduce

-- e.g. hdfs dfs -mkdir /user/cloudera/pigoutnew



-- ************* THE CODE STARTS HERE: ************************

wordfile = LOAD '/user/cloudera/pigin/testfile*' USING PigStorage('\n') as (linesin:chararray);

wordfile_flat = FOREACH wordfile GENERATE FLATTEN(TOKENIZE(linesin)) as wordin;

wordfile_grpd = GROUP wordfile_flat by wordin;

word_counts = FOREACH wordfile_grpd GENERATE group, COUNT(wordfile_flat.wordin);



--If you are running pig -x mapreduce don't forget to

--use hdfs commands to rm the file and rmdir for the output



STORE word_counts into '/user/cloudera/pigoutnew/word_counts_pig';



-- And dont forget to copy from hdfs into your local unix filesystem, (but your paths may differ!)

--hdfs dfs -copyToLocal /user/cloudera/pigoutnew/word_counts_pig /home/cloudera/word_counts_pig

