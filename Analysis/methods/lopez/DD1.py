#!/usr/bin/env python
# On 20121226
# Monte Carlo to determine quantile
# by MLdP <lopezdeprado@lbl.gov>
from random import gauss
from numpy import zeros
from scipy.stats import scoreatpercentile,norm
#---------------------------------------------------------------
def main():
	#1) Parameters
	size=1e6 # size of the Monte Carlo experiment
	phi=.5 # AR(1) coefficient
	mu=1 # unconditional mean
	sigma=2 # Standard deviation of the random shock
	dPi0=1 # Bet at origin (initialization of AR(1))
	bets=25 # Number of bets in the cumulative process
	confidence=.95 # Confidence level for quantile
	#2) Compute exact solution
	q=getQ(bets,phi,mu,sigma,dPi0,confidence)
	#3) Simulate Monte Carlo paths
	pnl=zeros(int(size))
	for i in range(int(size)):pnl[i]=getPath(phi,mu,sigma,dPi0,bets)
	#4) Compute empirical quantile, based on Monte Carlo paths
	q_mc=scoreatpercentile(pnl,(1-confidence)*100)
	#5) Report difference
	print 'Quantile (Exact) = '+str(q)
	print 'Quantile (Monte Carlo) = '+str(q_mc)
	print 'Difference = '+str(q-q_mc)
	return
#---------------------------------------------------------------
def getQ(bets,phi,mu,sigma,dPi0,confidence):
	# Compute analytical solution to quantile
	#1) Mean
	mean=(phi**(bets+1)-phi)/(1-phi)*(dPi0-mu)+mu*bets
	#2) Variance
	var=sigma**2/(phi-1)**2
	var*=(phi**(2*(bets+1))-1)/(phi**2-1)-2*(phi**(bets+1)-1)/(phi-1)+bets+1
	#3) Quantile
	q=mean+norm.ppf(1-confidence,0,1)*var**.5
	return q
#---------------------------------------------------------------
def getPath(phi,mu,sigma,dPi0,bets):
	# Run path for the Monte Carlo
	delta,pnl=dPi0,0
	for i in range(int(bets)):
		delta=(1-phi)*mu+gauss(0,1)*sigma+delta*phi
		pnl+=delta
	return pnl
#---------------------------------------------------------------
# Boilerplate
if __name__=='__main__':main()