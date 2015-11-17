# -*- coding: utf-8 -*-
"""
Simple Join in Spark

Created on Mon Nov 16 16:49:48 2015

@author: michael.hunt
"""

def split_fileA(line):
    # split the input line in word and count on the comma
    word,count=line.split(",")
    # turn the count to an integer  
    count=int(count)
    return (word, count)
    
def split_fileB(line):
    # split the input line into word, date and count_string
    wordDate,count_string=line.split(",")
    #count=int(count)
    date,word=wordDate.split(" ")
    return (word, date + " " + count_string)
    