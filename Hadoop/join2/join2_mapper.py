# -*- coding: utf-8 -*-
"""
Created on Wed Nov 11 16:10:23 2015

@author: michael.hunt
"""

#!/usr/bin/env python
import sys



#list_of_files = open('join2gen*.txt','r')
#print list_of_files

for line in sys.stdin:
    print line
    line       = line.strip()   #strip out carriage return
    key_value  = line.split(",")   #split line, into key and value, returns a list
    key_in     = key_value[0]#.split(" ")   #key is first item in list
    value_in   = key_value[1]   #value is 2nd item 
    
    if isinstance(value_in, (int, long))==True:
        print( '%s\t%s' % (key_in, value_in))
    else:
        if value_in == 'ABC':
            print( '%s\t%s' % ( value_in, key_in))


