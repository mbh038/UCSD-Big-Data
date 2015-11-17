## SIMPLE JOIN IN SPARK

# Hadoop Platform and Application Framework
# Week 5
# Assignment One

# November 2015

# Michael Hunt

#################################################

# Start python
PYSPARK_DRIVER_PYTHON=ipython pyspark

# load data sets
fileA = sc.textFile("input/join1_FileA.txt")
fileB = sc.textFile("input/join1_FileB.txt")

fileA.collect()
fileB.collect()

# Mapper for File A

def split_fileA(line):
    # split the input line in word and count on the comma
    word,count=line.split(",")
    # turn the count to an integer  
    count=int(count)
    return (word, count)

test_line = "able,991"
split_fileA(test_line)  # split file A is a python function.

# map transformation
fileA_data = fileA.map(split_fileA)
fileA_data.collect()

# Mapper for File B

def split_fileB(line):
    # split the input line into word, date and count_string
    wordDate,count_string=line.split(",")
    #count=int(count)
    date,word=wordDate.split(" ")
    return (word, date + " " + count_string)

fileB_data = fileB.map(split_fileB)   
fileB_data.collect() 

# run join
fileB_joined_fileA = fileB_data.join(fileA_data)

# verify the result
fileB_joined_fileA.collect()


