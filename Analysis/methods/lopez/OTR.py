#!/usr/bin/env python
# Profit-taking and stop-loss simulations
# On 20131003 by MLdP <lopezdeprado@lbl.gov> 
import numpy as np
from random import gauss
from itertools import product
#----------------------------------------------------------------------------------------
def main():
    rPT=rSLm=np.linspace(0,10,21)
    count=0
    for prod_ in product([10,5,0,-5,-10],[5,10,25,50,100]):
        count+=1
        coeffs={'forecast':prod_[0],'hl':prod_[1],'sigma':1}
        output=batch(coeffs,nIter=1e5,maxHP=100,rPT=rPT,rSLm=rSLm)
    return output
#----------------------------------------------------------------------------------------
def batch(coeffs,nIter=1e5,maxHP=100,rPT=np.linspace(0,10,21), \
    rSLm=np.linspace(0,10,21),seed=0):
    phi,output1=2**(-1./coeffs['hl']),[]
    for comb_ in product(rPT,rSLm):
        output2=[]
        for iter_ in range(int(nIter)):
            p,hp,count=seed,0,0
            while True:
                p=(1-phi)*coeffs['forecast']+phi*p+coeffs['sigma']*gauss(0,1)
                cP=p-seed;hp+=1
                if cP>comb_[0] or cP<-comb_[1] or hp>maxHP:
                    output2.append(cP)
                    break
        mean,std=np.mean(output2),np.std(output2)
        print comb_[0],comb_[1],mean,std,mean/std
        output1.append((comb_[0],comb_[1],mean,std,mean/std))
    return output1