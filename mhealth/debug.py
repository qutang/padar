from api import M

def main():
    root = 'D:/Data/spades-lab'

    s = M(root).summarize(use_parallel=True, verbose=True).summary
    # s = M(root).summarize_partial('SPADES_24/MasterSynced', use_parallel=False, verbose=True)

    # print(s)
    s.to_csv(root + '/summary.csv', float_format = '%.3f', index=False)

if __name__ == '__main__':
    main()