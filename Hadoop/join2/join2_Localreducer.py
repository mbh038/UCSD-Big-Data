# -*- coding: utf-8 -*-
"""
Created on Wed Nov 11 16:55:38 2015

@author: michael.hunt
"""

#!/usr/bin/env python
#import sys


ABC_dict={}

# see https://docs.python.org/2/tutorial/datastructures.html for list details

line_cnt = 0  #count input lines
 
mapResult=open('mapOutput.txt','r')
for line in mapResult:
    line       = line.strip()       #strip out carriage return
    key_value  = line.split('\t')   #split line, into key and value, returns a list
    line_cnt   = line_cnt+1
    
    if key_value[0]=="ABC":
        if ABC_dict.has_key(key_value[1])==False:
            ABC_dict.update({key_value[1]:0})
mapResult.close()

line_cnt = 0  #count input lines
mapResult=open('mapOutput.txt','r')
for line in mapResult:
    line       = line.strip()       #strip out carriage return
    key_value  = line.split('\t')   #split line, into key and value, returns a list
    line_cnt   = line_cnt+1
    
    if ABC_dict.has_key(key_value[0]):
        ABC_dict[key_value[0]]+=int(key_value[1])

for key, value in ABC_dict.iteritems() :
    #print str(key)+" "+str(value) 
    print( '%s %s' % (key, value))
        
mapResult.close()