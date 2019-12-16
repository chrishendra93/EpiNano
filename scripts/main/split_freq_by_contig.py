#!/usr/bin/env python2.7
import os
from collections import defaultdict
from collections import OrderedDict
from tqdm import tqdm
from multiprocessing import Pool
from glob import glob
import pandas as pd
from time import time

#cc6m_2244_T7_ecorv,40,T,6.0,0,0,0,11:7:3:11:11:7
#ref,ref_pos,ref_base,depth,mis,ins,del,Qs


# mem = defaultdict(lambda: defaultdict(list))
# ks = OrderedDict()
# for l in fin.input():
#     ary = l.strip().split(',')
#     k = ','.join(ary[:3])
#     ks[k] = True
#     c,m,i,d = (ary[3:7])
#     c_m_i_d = np.array (map(float,[c,m,i,d]))
#     q_lst = map (float, ary[7].split(':'))
#     mem[k]['var'] = mem[k].get ('var',np.array((0,))) + c_m_i_d
#     mem[k]['q'] = mem[k].get ('q',[]) + q_lst

# # print '#Ref,pos,base,cov,q_mean,q_median,q_std,mis,ins,del'
# for k in ks:
#     cov = mem[k]['var'][0]
#     q_lst = mem[k]['q']
    # print ",".join ([k, str(cov),  ",".join (map (str, ['%0.3f' % np.mean(q_lst),'%0.3f'%np.median(q_lst),'%0.3f'%np.std(q_lst)])), ",".join (map (str, ['%0.3f'% x for x in list (mem[k]['var'])[1:]/cov] ))] )

def save_contig(task):
    fpath, out_dir = task
    try:
        freq_df = pd.read_csv(fpath, header=None)
    except Exception:
        return
    if len(freq_df) == 0:
        return
    else:
        for tx, df in freq_df.groupby(0):
            save_dir = os.path.join(out_dir, tx)
            if not os.path.exists(save_dir):
                os.mkdir(save_dir)
            fpath = os.path.join(save_dir, "{}.csv".format(time()))
            df.to_csv(fpath, index=False)


if __name__ == '__main__':

    input_dir = "/mnt/volume1/epinano_preprocessing/epinano_freq"
    out_dir = "/mnt/volume1/epinano_preprocessing/epinano_freq_by_tx"

    tasks = [(fpath, out_dir) for fpath in glob("{}/*.freq".format(input_dir))]

    if not os.path.exists(out_dir):
        os.mkdir(out_dir)

    with Pool(8) as p:
        for _ in tqdm(p.imap_unordered(save_contig, tasks), total=len(tasks)):
            pass
