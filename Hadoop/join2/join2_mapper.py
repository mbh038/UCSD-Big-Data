# -*- coding: utf-8 -*-
"""
Created on Wed Nov 11 16:10:23 2015

@author: michael.hunt
"""

#!/usr/bin/env python
import sys



for line in sys.stdin:
    print line
    line       = line.strip()   #strip out carriage return
    key_value  = line.split(",")   #split line, into key and value, returns a list
    key_in     = key_value[0]#.split(" ")   #key is first item in list
    value_in   = key_value[1]   #value is 2nd item 
    testNum=[int(s) for s in value_in.split() if s.isdigit()]
    
    if len(testNum)>0:
        print( '%s\t%s' % (key_in, value_in))
    else:
        if value_in == 'ABC':
            print( '%s\t%s' % ( value_in, key_in))


