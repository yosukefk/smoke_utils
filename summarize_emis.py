import pandas as pd
import smoke_reader as rdr
from pathlib import Path

ddir = Path('nei')
fnames = {
        'pt': '2020NEI_point_full_20230330.csv',
        'nonpt': 'SmokeFlatFile_NONPOINT_20230330.csv',
        'nr': 'SmokeFlatFile_NONROAD_20230330.csv', 
        'or': 'SmokeFlatFile_ONROAD_20230330.csv',
        }

# by species
lst = []
for kls,fn  in  fnames.items():
    print(fn)
    df = rdr.read_ff10(ddir / fn)
    o = df.groupby(['POLL']).agg({'ANN_VALUE':'sum'})
    o['klass'] = kls
    o = o.reset_index()
    lst.append(o)
df = pd.concat(lst).pivot(columns=['klass'], index='POLL').droplevel(0, axis=1)
df.to_csv('emis_by_klass_by_pollcode.csv')
# subset species
# by county
# by county/scc
