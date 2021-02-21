# by MLdP on 02/20/2014 <lopezdeprado@lbl.gov>
# Kinetic Component Analysis of a periodic function
import numpy as np,matplotlib.pyplot as pp,kca
from selectFFT import selectFFT
mainPath='../../'   
#---------------------------------------------------------
def getPeriodic(periods,nobs,scale,seed=0):
    t=np.linspace(0,np.pi*periods/2.,nobs)
    rnd=np.random.RandomState(seed)
    signal=np.sin(t)
    z=signal+scale*rnd.randn(nobs)
    return t,signal,z
#---------------------------------------------------------
def vsFFT():
    #1) Set parameters
    nobs,periods=300,10
    #2) Get Periodic noisy measurements
    t,signal,z=getPeriodic(periods,nobs,scale=.5)
    #3) Fit KCA
    x_point,x_bands=kca.fitKCA(t,z,q=.001)[:2]
    #4) Plot KCA's point estimates
    color=['b','g','r']
    pp.plot(t,z,marker='x',linestyle='',label='measurements')
    pp.plot(t,x_point[:,0],marker='o',linestyle='-',label='position', \
        color=color[0])
    pp.plot(t,x_point[:,1],marker='o',linestyle='-',label='velocity', \
        color=color[1])
    pp.plot(t,x_point[:,2],marker='o',linestyle='-',label='acceleration', \
        color=color[2])
    pp.legend(loc='lower left',prop={'size':8})
    pp.savefig(mainPath+'Data/test/Figure1.png')
    #5) Plot KCA's confidence intervals (2 std)
    for i in range(x_bands.shape[1]):
        pp.plot(t,x_point[:,i]-2*x_bands[:,i],linestyle='-',color=color[i])
        pp.plot(t,x_point[:,i]+2*x_bands[:,i],linestyle='-',color=color[i])
    pp.legend(loc='lower left',prop={'size':8})
    pp.savefig(mainPath+'Data/test/Figure2.png')
    pp.clf();pp.close() # reset pylab
    #6) Plot comparison with FFT
    fft=selectFFT(z.reshape(-1,1),minAlpha=.05)
    pp.plot(t,signal,marker='x',linestyle='',label='Signal')
    pp.plot(t,x_point[:,0],marker='o',linestyle='-',label='KCA position')
    pp.plot(t,fft['series'],marker='o',linestyle='-',label='FFT position')
    pp.legend(loc='lower left',prop={'size':8})
    pp.savefig(mainPath+'Data/test/Figure3.png')
    return