#!/usr/bin/env python
# On 20131224 by lopezdeprado@lbl.gov
import numpy as np
import scipy.stats as ss
import matplotlib.pyplot as pp
from math import log,exp
#-----------------------------------------------------------------------
def main():
    t,alpha=100,.05
    dist0=np.linspace(0.01,1.99,100)
    z=ss.norm.ppf(alpha)
    dist1=[]
    for dist0_ in dist0:
        corr0_=1-dist0_**2/2.
        u_=.5*log((1+corr0_)/(1-corr0_))-z/(t-3)**.5
        corr1_=(exp(2*u_)-1)/(exp(2*u_)+1)
        dist1_=(2*(1-corr1_))**.5
        dist1.append(dist1_)
    pp.plot(dist0,dist1)
    pp.plot(dist0,dist0,'--r')
    pp.show()
    return
#-----------------------------------------------------------------------
if __name__=='__main__':main()