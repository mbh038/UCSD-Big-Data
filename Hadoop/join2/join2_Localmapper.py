# -*- coding: utf-8 -*-
"""
Created on Wed Nov 11 16:10:23 2015

@author: michael.hunt
"""

#!/usr/bin/env python
#import sys



list_of_files = ['join2_gennumA.txt','join2_gennumB.txt','join2_gennumC.txt','join2_genchanA.txt','join2_genchanB.txt','join2_genchanC.txt']

#fileOut=open('mapOutput.txt','w')

outFile=open('mapOutput.txt','w')
for files in list_of_files:
    nextFile=open (files,'r')
    fileString=nextFile.readlines()
    nextFile.close()
    #print files
    for line in fileString:
        #print line
        line       = line.strip()   #strip out carriage return
        key_value  = line.split(",")   #split line, into key and value, returns a list
        key_in     = key_value[0]#.split(" ")   #key is first item in list
        value_in   = key_value[1]   #value is 2nd item 
        testNum=[int(s) for s in value_in.split() if s.isdigit()]
        

        if len(testNum)>0:
            #print type(value_in)
            print( '%s\t%s' % (key_in, value_in))
            outFile.write( '%s\t%s\n' % (key_in, value_in))
        else:
            if value_in == 'ABC':
                print( '%s\t%s' % ( value_in, key_in))
                outFile.write( '%s\t%s\n' % ( value_in, key_in))


outFile.close()