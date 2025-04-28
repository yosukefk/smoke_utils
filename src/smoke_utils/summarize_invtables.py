import smoke_reader as rdr
import pandas as pd
from functools import reduce

dfs = {}
# standard
dfs['s_pt'] = rdr.read_invtable('invtables/invtable_standard/invtable_2014platform_integrate_21dec2018_v3.txt')
dfs['s_pt'] = rdr.read_invtable('invtables/invtable_standard/invtable_2014platform_nointegrate_01may2019_v2.txt')
dfs['s_pt'] = rdr.read_invtable('invtables/invtable_standard/invtable_MOVES2014_custom_speciation_20sep2022_v4.txt')

#hap
dfs['h_pt'] = rdr.read_invtable('invtables/invtable_hapcap/invtable_2017_NATA_CMAQ_22feb2023_nf_v7.txt',)
dfs['h_ar'] = rdr.read_invtable('invtables/invtable_hapcap/invtable_2017_NATA_CMAQ_26apr2023_v7.txt',)
dfs['h_mb'] = rdr.read_invtable('invtables/invtable_hapcap/invtable_MOVES2014_custom_speciation_24apr2023_v6.txt',)

# get all the poll code
def add_columns(df, hap, knd):
    df['hap'] = hap
    df['kind'] = knd
    return df

dfs = {k:add_columns(v, k[0], k[2:]) for k,v in dfs.items()}

df_all = pd.concat(dfs.values()).reset_index()

df_pair = df_all.loc[:, ['dataname', 'pollcode', 'hap','kind' ]].drop_duplicates().to_csv('pollcode_dataname_map.csv', index=False)




#p = [_.pollcode.unique() for _ in dfs.values()]
#p = reduce( lambda p,q: p|q, (set(_) for _ in p),)
