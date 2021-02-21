#!/usr/bin/env python
# EF3M class for the exact fit of a mixture of two Gaussians
# On 20120217 by MLdP <lopezdeprado@lbl.gov>

import random

#-------------------------------------------
moments=[0.7,2.6,0.4,25,-59.8] # about the origin
epsilon=10**-5
factor=2

#-------------------------------------------
class M2N:
    def __init__(self,moments):
        self.moments=moments
        self.parameters=[0 for i in range(5)]
        self.error=sum([moments[i]**2 for i in range(len(moments))])

    def fit(self,mu2,epsilon):
        p1=random.random()
        numIter=0
        while True:
            numIter+=1
            try:
                #parameters=iter4(mu2,p1,self.moments)
                parameters=iter5(mu2,p1,self.moments)
            except:
                return
            moments=get_moments(parameters)
            error=sum([(self.moments[i]-moments[i])**2 for i in range(len(moments))])
            if error<self.error:
                self.parameters=parameters
                self.error=error
            if abs(p1-parameters[4])<epsilon:return
            if numIter>1/epsilon:return
            p1=parameters[4]
            mu2=parameters[1] # for the 5th moment's convergence
        
#-------------------------------------------
def get_moments(parameters):
    m1=parameters[4]*parameters[0]+(1-parameters[4])*parameters[1]
    m2=parameters[4]*(parameters[2]**2+parameters[0]**2)+(1-parameters[4])* \
        (parameters[3]**2+parameters[1]**2)
    m3=parameters[4]*(3*parameters[2]**2*parameters[0]+parameters[0]**3)+ \
        (1-parameters[4])*(3*parameters[3]**2*parameters[1]+parameters[1]**3)
    m4=parameters[4]*(3*parameters[2]**4+6*parameters[2]**2*parameters[0]**2+ \
        parameters[0]**4)+(1-parameters[4])*(3*parameters[3]**4+6*parameters[3]**2* \
        parameters[1]**2+parameters[1]**4)
    m5=parameters[4]*(15*parameters[2]**4*parameters[0]+10*parameters[2]**2* \
        parameters[0]**3+parameters[0]**5)+(1-parameters[4])*(15*parameters[3]**4* \
        parameters[1]+10*parameters[3]**2*parameters[1]**3+parameters[1]**5)
    return [m1,m2,m3,m4,m5]

#-------------------------------------------
def iter4(mu2,p1,moments):
    mu1=(moments[0]-(1-p1)*mu2)/p1
    sigma2=((moments[2]+2*p1*mu1**3+(p1-1)*mu2**3-3*mu1*(moments[1]+mu2**2* \
        (p1-1)))/(3*(1-p1)*(mu2-mu1)))**(.5)
    sigma1=((moments[1]-sigma2**2-mu2**2)/p1+sigma2**2+mu2**2-mu1**2)**(.5)
    p1=(moments[3]-3*sigma2**4-6*sigma2**2*mu2**2-mu2**4)/(3*(sigma1**4-sigma2**4)+ \
        6*(sigma1**2*mu1**2-sigma2**2*mu2**2)+mu1**4-mu2**4)
    return [mu1,mu2,sigma1,sigma2,p1]

#-------------------------------------------
def iter5(mu2,p1,moments):
    mu1=(moments[0]-(1-p1)*mu2)/p1
    sigma2=((moments[2]+2*p1*mu1**3+(p1-1)*mu2**3-3*mu1*(moments[1]+mu2**2* \
        (p1-1)))/(3*(1-p1)*(mu2-mu1)))**(.5)
    sigma1=((moments[1]-sigma2**2-mu2**2)/p1+sigma2**2+mu2**2-mu1**2)**(.5)
    a=(6*sigma2**4+(moments[3]-p1*(3*sigma1**4+6*sigma1**2*mu1**2+mu1**4))/ \
        (1-p1))**.5
    mu2=(a-3*sigma2**2)**.5
    a=15*sigma1**4*mu1+10*sigma1**2*mu1**3+mu1**5
    b=15*sigma2**4*mu2+10*sigma2**2*mu2**3+mu2**5
    p1=(moments[4]-b)/(a-b)
    return [mu1,mu2,sigma1,sigma2,p1]

#-------------------------------------------
def binomialCoeff(n, k):
    if k<0 or k>n:return 0
    if k>n-k:k=n-k
    c = 1
    for i in range(k):
        c=c*(n-(k-(i+1)))
        c=c//(i+1)
    return c

#-------------------------------------------
def centeredMoment(moments,order):
    moment_c=0
    for j in range(order+1):
        comb=binomialCoeff(order,j)
        if j==order:
            a=1
        else:
            a=moments[order-j-1]
        moment_c+=(-1)**j*comb*moments[0]**j*a
    return moment_c

#-------------------------------------------
def main():
    stDev=centeredMoment(moments,2)**.5
    mu2=[float(i)*epsilon*factor*stDev+moments[0] for i in range(1,int(1/epsilon))]
    m2n=M2N(moments)
    err_min=m2n.error
    for i in mu2:
        m2n.fit(i,epsilon)
        if m2n.error<err_min:
            print m2n.parameters, m2n.error
            err_min=m2n.error

#-------------------------------------------
if __name__=='__main__': main()