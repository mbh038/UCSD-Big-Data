# -*- coding: utf-8 -*-
"""
Created on Thu Nov 12 04:58:47 2015

@author: Mike
"""
def make_join2Local(fileName,arg1,lines,ri):
    #!/usr/bin/env python
    import sys
    
    # --------------------------------------------------------------------------
    #  (make_join2data.py) Generate a random combination of titles and viewer counts, or channels
    # this is a simple version of a congruential generator, 
    #   not a great random generator but enough  
    # --------------------------------------------------------------------------
    
    
    file = open(fileName, "w")

    chans   = ['ABC','DEF','CNO','NOX','YES','CAB','BAT','MAN','ZOO','XYZ','BOB']
    sh1 =['Hot','Almost','Hourly','PostModern','Baked','Dumb','Cold','Surreal','Loud']
    sh2 =['News','Show','Cooking','Sports','Games','Talking','Talking']
    vwr =range(17,1053)
    
    chvnm=arg1  #get number argument, if its n, do numbers not channels,
    
    lch=len(chans)
    lsh1=len(sh1)
    lsh2=len(sh2)
    lvwr=len(vwr)
    ci=1
    s1=2
    s2=3
    vwi=4
    ri=int(ri)
    for i in range(0,int(lines)):  #arg 2 is the number of lines to output
    
        if chvnm=='n':  #no numuber
            print('{0}_{1},{2}'.format(sh1[s1],sh2[s2],chans[ci]))
            file.write('{0}_{1},{2}\n'.format(sh1[s1],sh2[s2],chans[ci]))
        else:
            print('{0}_{1},{2}'.format(sh1[s1],sh2[s2],vwr[vwi])) 
            file.write('{0}_{1},{2}\n'.format(sh1[s1],sh2[s2],vwr[vwi]))
            
        ci=(5*ci+ri) % lch   
        s1=(4*s1+ri) % lsh1
        s2=(3*s1+ri+i) % lsh2
        vwi=(2*vwi+ri+i) % lvwr
     
        if (vwi==4): vwi=5
            
    file.close()
        
#python make_join2data.py y 1000 13 > join2_gennumA.txt
#python make_join2data.py y 2000 17 > join2_gennumB.txt
#python make_join2data.py y 3000 19 > join2_gennumC.txt
#python make_join2data.py n 100  23 > join2_genchanA.txt
#python make_join2data.py n 200  19 > join2_genchanB.txt
#python make_join2data.py n 300  37 > join2_genchanC.txt
        
make_join2Local("join2_gennumA.txt","y",1000,13)
make_join2Local("join2_gennumB.txt","y",2000,17)
make_join2Local("join2_gennumC.txt","y",3000,19)
make_join2Local("join2_genchanA.txt","n",100,23)
make_join2Local("join2_genchanB.txt","n",200,19)
make_join2Local("join2_genchanC.txt","n",300,37)