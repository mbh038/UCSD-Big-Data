## ADVANCED JOIN IN SPARK

# Hadoop Platform and Application Framework
# Week 5
# Assignment Two

# November 2015

# Michael Hunt

## Goal of the programming assignment

# gennum files contain show names and their viewers, genchan files contain show names and their
# channel. We want to find out the total number of viewer across all shows for the channel BAT.

#################################################

# Start python
PYSPARK_DRIVER_PYTHON=ipython pyspark

# Verify input data
hdfs dfs -ls input/

# Read shows files
show_views_file = sc.textFile("input/join2_gennum?.txt")

# view first two lines
show_views_file.take(2)

# Parse shows files
def split_show_views(line):
    """
    Function to split and parse each line of the data set
    line: 'show,views' a string from a gennum file
    """
    # split the input line in word and count on the comma
    show,views=line.split(",")
    # turn the count to an integer  
    views=int(views)
    return (show, views)
    
show_views = show_views_file.map(split_show_views)

# view the result
show_views.collect()

# view just the first two lines
show_views.take(2)

# Read channel files
show_channel_file = sc.textFile("input/join2_genchan?.txt")

# view first two lines
show_channel_file.take(2)

# Parse channel files
def split_show_channel(line):
    """
    Function to split and parse each line of the data set
    line: 'show,channel' a string from a gennum file
    """
    show,channel=line.split(",")
    return (show, channel)

show_channel = show_channel_file.map(split_show_channel)    

# view the result
show_channel.take(2)

# Join the two data sets
# use the join transformation, order of files does not matter
joined_dataset = show_views.join(show_channel)
joined_dataset = show_channel.join(show_views)

# view the result
joined_dataset.take(2)

# Extract channel as key
# want total viewers by channel

def extract_channel_views(show_views_channel): 
    """
    Aim is to find the total viewers by channel
    show_views_channel: 'show', (views, 'channel')
    returns:an RDD with the channel as key and all the viewer counts, whichever
    is the show.
    """
    channel,views=show_views_channel[1]
    return (channel, views)
    
channel_views = joined_dataset.map(extract_channel_views)

def sum_channel_viewers(a,b):
    return a + b

channel_views.reduceByKey(sum_channel_viewers).collect()