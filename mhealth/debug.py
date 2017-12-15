from api import M

root = '/Users/qutang/Projects/data/spades_lab_fixed'

s = M(root).summarize(use_parallel=True, verbose=True).summary
# s = M(root).summarize_partial('SPADES_1', use_parallel=True, verbose=True)

print(s)
# s.write_csv(root + '/summary.csv', float_format = '%.3f', index=False)