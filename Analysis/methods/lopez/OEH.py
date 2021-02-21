#!/usr/bin/env python
# By MLdP on 20120316 <ml863@cornell.edu>
# It computes the optimal execution horizon

#---------------------------------------------------------
# PARAMETERS
sigma=1000
volSigma=10000
S_S=10000
zLambda=-1.644853627 #CDF(0.05) from the Std Normal dist
vB=0.5
phi=1
m=1000
k=0

#---------------------------------------------------------
def signum(int):
    if(int < 0):return -1
    elif(int > 0):return 1
    else:return 0

#---------------------------------------------------------
def getOI(v,m,phi,vB,sigma,volSigma):
    return phi*(float(m-(2*vB-1)*abs(m))/v+2*vB-1)+ \
        (1-phi)*(2*vB-1)

#---------------------------------------------------------
def getBounds(m,phi,vB,sigma,volSigma,S_S,zLambda,k=0):
    vB_l=float(signum(m)+1)/2-zLambda*sigma*abs(m)**0.5/ \
        float(4*phi*(S_S+abs(m)*k)*volSigma**0.5)
    vB_u=float(signum(m)+1)/2+zLambda*sigma*abs(m)**0.5/ \
        float(4*phi*(S_S+abs(m)*k)*volSigma**0.5)
    vB_z=(signum(m)*phi/float(phi-1)+1)/2.
    return vB_l,vB_u,vB_z

#---------------------------------------------------------
def minFoot(m,phi,vB,sigma,volSigma,S_S,zLambda,k=0):
    # compute vB boundaries:
    if phi<=0:phi+=10**-12
    if phi>=1:phi-=10**-12
    vB_l,vB_u,vB_z=getBounds(m,phi,vB,sigma,volSigma,S_S, \
        zLambda,k)
    
    # try alternatives
    if (2*vB-1)*abs(m)<m:
        v1=(2*phi*((2*vB-1)*abs(m)-m)*(S_S+abs(m)*k)*volSigma**0.5/ \
            float(zLambda*sigma))**(2./3)
        oi=getOI(v1,m,phi,vB,sigma,volSigma)
        if oi>0:
            if vB<=vB_u: return v1
            if vB>vB_u: return abs(m)
    elif (2*vB-1)*abs(m)>m:
        v2=(2*phi*(m-(2*vB-1)*abs(m))*(S_S+abs(m)*k)*volSigma**0.5/ \
            float(zLambda*sigma))**(2./3)
        oi=getOI(v2,m,phi,vB,sigma,volSigma)
        if oi<0:
            if vB>=vB_l: return v2
            if vB<vB_l: return abs(m)
    elif (2*vB-1)*abs(m)==m: return abs(m)
    if m<0:
        if vB<vB_z: return phi*(abs(m)-m/float(2*vB-1))
        if vB>=vB_z: return abs(m)
    else:
        if vB>=vB_z: return phi*(abs(m)-m/float(2*vB-1))
        if vB<vB_z: return abs(m)

#---------------------------------------------------------
def main():
    print minFoot(m,phi,vB,sigma,volSigma,S_S,zLambda,k)
  
#---------------------------------------------------------
if __name__ == '__main__': main()