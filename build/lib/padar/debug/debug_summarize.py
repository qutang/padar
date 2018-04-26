from mhealth.api import M

def main():
    root = 'F:/data/spades-2day'

    s = M(root).summarize('SPADES_1/MasterSynced', use_parallel=False, verbose=True)

    # print(s)
    s.to_csv(root + '/summary.csv', float_format = '%.3f', index=False)

if __name__ == '__main__':
    main()