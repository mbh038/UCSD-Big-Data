# -*- coding: utf-8 -*-
"""
Advanced Join in Spark

Created on Tue Nov 17 08:21:29 2015

@author: michael.hunt
"""

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
    
testViews="Hello,21"

print split_show_views(testViews)


def split_show_channel(line):
    """
    Function to split and parse each line of the data set
    line: 'show,channel' a string from a gennum file
    """
    show,channel=line.split(",")
    return (show, channel)
    
testChannel="Hello,BAT"

print split_show_channel(testChannel)

def extract_channel_views(show_views_channel): 
    """
    Aim is to find the total viewers by channel
    show_views_channel: 'show', (views, 'channel')
    returns:an RDD with the channel as key and all the viewer counts, whichever
    is the show.
    """
    channel,views,=show_views_channel[1]
    return (channel, views)
    
testExtractCV=['Baked_Talking', (132, 'MAN')]

print extract_channel_views(testExtractCV)


def sum_channel_viewers(a,b):
    return a+ b
    
