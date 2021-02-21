# by MLdP on 02/20/2014 <lopezdeprado@lbl.gov>
# FFT signal extraction with frequency selection
import numpy as np
import statsmodels.stats.diagnostic as sm3
#---------------------------------------------------------
def selectFFT(series,minAlpha=None):
    # Implements a forward algorithm for selecting FFT frequencies
    #1) Initialize variables
    series_=series
    fftRes=np.fft.fft(series_,axis=0)
    fftRes={i:j[0] for i,j in zip(range(fftRes.shape[0]),fftRes)}
    fftOpt=np.zeros(series_.shape,dtype=complex)
    lags,crit=int(12*(series_.shape[0]/100.)**.25),None
    #2) Search forward
    while True:
        key,critOld=None,crit
        for key_ in fftRes.keys():
            fftOpt[key_,0]=fftRes[key_]
            series__=np.fft.ifft(fftOpt,axis=0)
            series__=np.real(series__)
            crit_=sm3.acorr_ljungbox(series_-series__,lags=lags) # test for the max # lags
            crit_=crit_[0][-1],crit_[1][-1]
            if crit==None or crit_[0]<crit[0]:crit,key=crit_,key_
            fftOpt[key_,0]=0
        if key!=None:
            fftOpt[key,0]=fftRes[key]
            del fftRes[key]
        else:break
        if minAlpha!=None:
            if crit[1]>minAlpha:break
            if critOld!=None and crit[0]/critOld[0]>1-minAlpha:break
    series_=np.fft.ifft(fftOpt,axis=0)
    series_=np.real(series_)
    out={'series':series_,'fft':fftOpt,'res':fftRes,'crit':crit}
    return out