# by MLdP on 02/20/2014 <lopezdeprado@lbl.gov>
# Kinetic Component Analysis of a periodic function
import numpy as np,matplotlib.pyplot as pp,kca
import statsmodels.nonparametric.smoothers_lowess as sml
mainPath='../../'   
#---------------------------------------------------------
def vsLOWESS():
    # by MLdP on 02/24/2014 <lopezdeprado@lbl.gov>
    # Kinetic Component Analysis of a periodic function
    #1) Set parameters
    nobs,periods,frac=300,10,[.5,.25,.1]
    #2) Get Periodic noisy measurements
    t,signal,z=getPeriodic(periods,nobs,scale=.5)
    #3) Fit KCA
    x_point,x_bands=kca.fitKCA(t,z,q=.001)[:2]
    #4) Plot comparison with LOWESS
    pp.plot(t,z,marker='o',linestyle='',label='measurements')
    pp.plot(t,signal,marker='x',linestyle='',label='Signal')
    pp.plot(t,x_point[:,0],marker='o',linestyle='-',label='KCA position')
    for frac_ in frac:
        lowess=sml.lowess(z.flatten(),range(z.shape[0]),frac=frac_)[:,1].reshape(-1,1)
        pp.plot(t,lowess,marker='o',linestyle='-',label='LOWESS('+str(frac_)+')')
    pp.legend(loc='lower left',prop={'size':8})
    pp.savefig(mainPath+'Data/test/Figure4.png')
    return