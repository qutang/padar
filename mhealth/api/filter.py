from scipy.signal import butter, filtfilt

def butterworth(df, sr, cutoffs, order, btype='highpass'):
    '''Apply butterworth filter to the input sensor data frame each column'''
    nyquist = sr / 2.0

    if(isinstance(cutoffs, list)):
        cutoffs = [float(cutoff) / nyquist for cutoff in cutoffs]
        B,A = butter(order,cutoffs,btype=btype,output='ba')
    else:
        B,A = butter(order,float(cutoffs)/nyquist,btype=btype,output='ba')
    cols = df.columns[1:]
    df[cols] = filtfilt(B,A,df[cols].values, axis=1, padtype=None)
    return df