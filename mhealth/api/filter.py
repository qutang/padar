from scipy.signal import butter, filtfilt

def butterworth(df, sr, cutoffs, order, type='highpass'):
    '''Apply butterworth filter to the input sensor data frame each column'''
    nyquist = sr / 2.0

    if(isinstance(cutoffs, list)):
        cutoffs = [cutoff / nyquist for cutoff in cutoffs]
        B,A = butter(order,cutoffs,btype=type,output='ba')
    else:
        B,A = butter(order,cutoffs/nyquist,btype=type,output='ba')
    cols = df.columns[1:]
    df[cols] = filtfilt(B,A,df[cols].values, axis=1, padtype=None)
    return df