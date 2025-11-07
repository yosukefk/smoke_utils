#!/usr/bin/env python
import pandas as pd
import re

strcol = ['poll', 'control_ids', 'reg_codes', 'agy_facility_id', 'agy_unit_id','design_capacity_units', 'oris_boiler_id', 'zipcode', 'emis_type',]

known_headers = {
        'FF10_HOURLY_POINT': "country,region_cd,tribal_code,facility_id,unit_id,rel_point_id,process_id,scc,poll,op_type_cd,calc_method,date_updated,date,daytot,hrval0,hrval1,hrval2,hrval3,hrval4,hrval5,hrval6,hrval7,hrval8,hrval9,hrval10,hrval11,hrval12,hrval13,hrval14,hrval15,hrval16,hrval17,hrval18,hrval19,hrval20,hrval21,hrval22,hrval23,comment", 
        'FF10_DAILY_POINT' : "country_cd,region_cd,tribal_code,facility_id,unit_id,rel_point_id,process_id,scc,poll,op_type_cd,calc_method,date_updated,monthnum,monthtot,dayval1,dayval2,dayval3,dayval4,dayval5,dayval6,dayval7,dayval8,dayval9,dayval10,dayval11,dayval12,dayval13,dayval14,dayval15,dayval16,dayval17,dayval18,dayval19,dayval20,dayval21,dayval22,dayval23,dayval24,dayval25,dayval26,dayval27,dayval28,dayval29,dayval30,dayval31,comment", 
        'FF10_POINT' : "country_cd,region_cd,tribal_code,facility_id,unit_id,rel_point_id,process_id,agy_facility_id,agy_unit_id,agy_rel_point_id,agy_process_id,scc,poll,ann_value,ann_pct_red,facility_name,erptype,stkhgt,stkdiam,stktemp,stkflow,stkvel,naics,longitude,latitude,ll_datum,horiz_coll_mthd,design_capacity,design_capacity_units,reg_codes,fac_source_type,unit_type_code,control_ids,control_measures,current_cost,cumulative_cost,projection_factor,submitter_id,calc_method,data_set_id,facil_category_code,oris_facility_code,oris_boiler_id,ipm_yn,calc_year,date_updated,fug_height,fug_width_xdim,fug_length_ydim,fug_angle,zipcode,annual_avg_hours_per_year,jan_value,feb_value,mar_value,apr_value,may_value,jun_value,jul_value,aug_value,sep_value,oct_value,nov_value,dec_value,jan_pctred,feb_pctred,mar_pctred,apr_pctred,may_pctred,jun_pctred,jul_pctred,aug_pctred,sep_pctred,oct_pctred,nov_pctred,dec_pctred,comment", 
        'FF10_NONPOINT' : "country_cd,region_cd,tribal_code,census_tract_cd,shape_id,scc,emis_type,poll,ann_value,ann_pct_red,control_ids,control_measures,current_cost,cumulative_cost,projection_factor,reg_codes,calc_method,calc_year,date_updated,data_set_id,jan_value,feb_value,mar_value,apr_value,may_value,jun_value,jul_value,aug_value,sep_value,oct_value,nov_value,dec_value,jan_pctred,feb_pctred,mar_pctred,apr_pctred,may_pctred,jun_pctred,jul_pctred,aug_pctred,sep_pctred,oct_pctred,nov_pctred,dec_pctred,comment", 
        'FF10_NONROAD' : "country_cd,region_cd,tribal_code,census_tract_cd,shape_id,scc,emis_type,poll,ann_value,ann_pct_red,control_ids,control_measures,current_cost,cumulative_cost,projection_factor,reg_codes,calc_method,calc_year,date_updated,data_set_id,jan_value,feb_value,mar_value,apr_value,may_value,jun_value,jul_value,aug_value,sep_value,oct_value,nov_value,dec_value,jan_pctred,feb_pctred,mar_pctred,apr_pctred,may_pctred,jun_pctred,jul_pctred,aug_pctred,sep_pctred,oct_pctred,nov_pctred,dec_pctred,comment", 
        'FF10_ONROAD' : "country_cd,region_cd,tribal_code,census_tract_cd,shape_id,scc,emis_type,poll,ann_value,ann_pct_red,control_ids,control_measures,current_cost,cumulative_cost,projection_factor,reg_codes,calc_method,calc_year,date_updated,data_set_id,jan_value,feb_value,mar_value,apr_value,may_value,jun_value,jul_value,aug_value,sep_value,oct_value,nov_value,dec_value,jan_pctred,feb_pctred,mar_pctred,apr_pctred,may_pctred,jun_pctred,jul_pctred,aug_pctred,sep_pctred,oct_pctred,nov_pctred,dec_pctred,comment", 
        'FF10_ACTIVITY' : "country_cd,region_cd,tribal_code,census_tract_cd,shape_id,scc,CD,MSR,activity_type,ann_parm_value,calc_year,date_updated,data_set_id,jan_value,feb_value,mar_value,apr_value,may_value,jun_value,jul_value,aug_value,sep_value,oct_value,nov_value,dec_value,comment",
        }

def snoop_ff10(fname):
    """read header part of FF10 inventory file"""
    try:
        return read_ff10(fname, snoop=True)
    except:
        return read_ff10(fname, snoop=True)

def head_ff10(fname):
    """read first 10 rec of FF10 inventory file"""
    return read_ff10(fname, nrows=10)

def process_ff10(fname, func, reducer=None, chunksize=10**6):
    results = read_ff10(fname, func=func, reducer=reducer, chunksize=chunksize)
    return results


def read_ff10(fname, snoop=False, nrows=None, use_embedded_header = True, func=None, reducer=None,chunksize=None):
    """reads FF10 inventory file"""
    try:
        return read_ff10_slave(fname, snoop, nrows, use_embedded_header, func, reducer,chunksize)

    except:
        # https://stackoverflow.com/questions/1035340/reading-binary-file-and-looping-over-each-byte
        import chardet
        with open(fname, 'rb') as f:
            result = chardet.detect(f.read(10**6))  # or readline if the file is large
        print(result)
        return read_ff10_slave(fname, snoop, nrows, use_embedded_header, func, reducer,chunksize, encoding=result['encoding'])


def read_ff10_slave(fname, snoop=False, nrows=None, use_embedded_header = True, func=None, reducer=None,chunksize=None,encoding='utf-8'):
    #print('enc', encoding)
    with open(fname, encoding=encoding) as f:
        h0 = None
        h1 = None
        nskip = 0
        h_save = []
        for line in f:
            if nskip == 0: 
                h0 = line.strip()
                #print(line)
            if line and (line[0] != '#' and line !='\n'): break
            h_save.append(line)
            nskip += 1


    h1 = line.strip()
    has_header = True
    if not h1.lower().startswith('country'):
        has_header = False
        for line in h_save[::-1]:
            m = re.match('^# +country', line, re.I)
            if m:
                h1 = line.strip()

    if snoop:
        return {'h0': h0, 'h1': h1, 'nskip': nskip, 'has_header': has_header}

    # lets just use known headers
    h = None
    for k,v in known_headers.items():
        if k in h0:
            h = v
            break
    if not h:
        raise ValueError(f'Unknown file type: {h0}')

    h = h.split(',')


    kwds = {'nrows': nrows, 'dtype' : {_:str for _ in strcol}, 'chunksize': chunksize, 'encoding':encoding}
    if use_embedded_header:
        if has_header:
            nskip += 1

        kwds |= {'skiprows':nskip, 'names': h, }

        #df = pd.read_csv(fname, skiprows=nskip, names=h, dtype={_:str for _ in strcol}, nrows=nrows)
    else:
        if has_header:
            kwds |= {'skiprows':nskip, }
            #df = pd.read_csv(fname, skiprows=nskip, dtype={_:str for _ in strcol}, nrows=nrows)
        else:
            kwds |= {'skiprows':nskip, 'names': h, }
            #df = pd.read_csv(fname, skiprows=nskip, names=h, dtype={_:str for _ in strcol}, nrows=nrows)
    if func is None:
        df = pd.read_csv(fname, **kwds)
        return df
    else:
        results = []
        with pd.read_csv(fname, **kwds) as reader:
            for chunk in reader:
                results.append(func(chunk))
        if not reducer is None:
            results = reducer(results)
        return results

def read_gref(fname):
    """reads GREF file"""
    with open(fname) as f:
        nskip = 0
        for line in f:
            if line[0] != '#': break
            nskip += 1
    df = pd.read_csv(fname, skiprows=nskip, delimiter=';', comment='!', names=['region_cd', 'scc', 'srg_cd'])
    df2 = pd.read_csv(fname, skiprows=nskip, delimiter='!', names=['data', 'comment'])
    df['comment'] = df2.comment
    
    return df

def read_srgdesc(fname):
    """read srgdesc file"""
    with open(fname) as f:
        nskip = 0
        for line in f:
            if line[0] != '#': break
            nskip += 1
    df = pd.read_csv(fname, skiprows=nskip, delimiter=';', comment='!', names=['country_cd', 'srg_cd', 'srg_desc', 'srg_file'])
    return df


def read_sccdesc(fname, lv=None):
    """reads SCCDESC file"""
    with open(fname) as f:
        nskip = 0
        for line in f:
            if line[0] != '#': break
            nskip += 1
    df = pd.read_csv(fname, skiprows=nskip, names=['scc', 'desc'])
    
    df['nchar'] = df.scc.str.len()
    
    df['scc_lv1'] = df.scc.str.slice(stop=-7)# // 10000000
    df['scc_lv2'] = df.scc.str.slice(stop=-5)# // 10000000
    df['scc_lv3'] = df.scc.str.slice(stop=-2)# // 100
    df.loc[df.nchar >= 10, 'scc_lv1'] = df.scc.str.slice(stop=-8)
    df.loc[df.nchar >= 10, 'scc_lv2'] = df.scc.str.slice(stop=-6)
    df.loc[df.nchar >= 10, 'scc_lv3'] = df.scc.str.slice(stop=-3)
    df = df.drop(columns='nchar')
    df[['desc_lv1', 'desc_lv2', 'desc_lv3', 'desc_lv4']] = df.desc.str.split(';', n=3, expand=True)
    if lv == 1:
        df = (df.loc[:, ['scc_lv1', 'desc_lv1']]
                .drop_duplicates()
                .assign(ky=lambda x: x['scc_lv1'].str.pad(10))
                .sort_values('ky')
                .drop(columns='ky')
                .reset_index(drop=True))
    elif lv == 2:
        df = (df.loc[:, ['scc_lv1', 'scc_lv2', 'desc_lv1', 'desc_lv2']]
                .drop_duplicates()
                .assign(ky=lambda x: x['scc_lv2'].str.pad(10))
                .sort_values('ky')
                .drop(columns='ky')
                .reset_index(drop=True))
    elif lv == 3:
        df = (df.loc[:, ['scc_lv1', 'scc_lv2', 'scc_lv3', 'desc_lv1', 'desc_lv2', 'desc_lv3']] 
                .drop_duplicates()
                .assign(ky=lambda x: x['scc_lv3'].str.pad(10))
                .sort_values('ky')
                .drop(columns='ky')
                .reset_index(drop=True))
    elif lv == 4:
        df['scc_lv4'] = df.scc
        df = (df.loc[:, ['scc_lv1', 'scc_lv2', 'scc_lv3', 'scc_lv4', 'desc_lv1', 'desc_lv2', 'desc_lv3', 'desc_lv4']]
                .drop_duplicates()
                .assign(ky=lambda x: x['scc_lv4'].str.pad(10))
                .sort_values('ky')
                .drop(columns='ky')
                .reset_index(drop=True))
    return df

def read_invtable(fname):
    """read INVTABLE file"""
    with open(fname) as f:
        nskip = 0
        for line in f:
            if line[0] != '#': break
            nskip += 1

    df = pd.read_fwf(fname, colspecs=[
        (0,11),
        (12,15),
        (16,32),
        (33,38),
        (39,40),
        (41,42),
        (43,49),
        (49,50),
        (51,52),
        (53,54),
        (57,60),
        (57,60),
        (61,77),
        (78,118),
        (118,158),
        ], 
        skiprows=nskip,
        names=[
            'dataname',
            'mode',
            'pollcode',
            'sp4id',
            'react',
            'keep',
            'fact',
            'voc',
            'modelspec',
            'explicit',
            'activity',
            'nti',
            'unit',
            'invdesc',
            'casdesc',
            ])
    return df

def read_costcy(fname):
    # TODO some columns should be character
    dct = {}
    with open(fname) as f:
        for i, line in enumerate(f):
            if line.startswith('/COUNTRY/'):
                pos_country = i
            elif line.startswith('/STATE/'):
                pos_state = i
            elif line.startswith('/COUNTY/'):
                pos_county = i

        f.seek(0)
        df_country = pd.read_fwf(f, 
                colspecs=[
                    (0,1),
                    (2,22),
                    ], 
                names = [
                    'country_cd',
                    'country_name',
                    ],
                skiprows=pos_country + 1,
                nrows = pos_state - pos_country - 1,
                )
        dct['country'] = df_country

        f.seek(0)
        df_state = pd.read_fwf(f,
                colspecs=[
                    (0,1),
                    (1,3),
                    (3,5),
                    (6,26),
                    (26,28),
                    (31,34),
                    ],
                skiprows=pos_state + 1,
                nrows = pos_county - pos_state - 1,
                names = [
                    'country_cd',
                    'state_cd',
                    'state_abbr',
                    'state_name',
                    'epa_region',
                    'state_tz',
                    ],
                )
        dct['state'] = df_state

        f.seek(0)
        df_county = pd.read_fwf(f,
                colspecs=[
                    (1,3),
                    (4,24),
                    (25,26),
                    (26,28),
                    (28,31),
                    (31,34),
                    (34,38),
                    (39,42),
                    (42,43),
                    (43,52),
                    (52,61),
                    (62,74),
                    (75,84),
                    (85,94),
                    (94,103),
                    (103,112),
                    (113,128),
                    ],
                names=[
                    'state_abbr',
                    'county_name',
                    'country_cd',
                    'state_cd',
                    'county_cd',
                    'state_cd_aeros',
                    'county_cd_aeros',
                    'county_tz',
                    'daylight_saving_time',
                    'county_center_longitude',
                    'county_center_latiitude',
                    'county_area',
                    'county_west_longitude',
                    'county_east_longitude',
                    'county_south_latitude',
                    'county_north_latitude',
                    'county_population',
                    ],
                skiprows=pos_county+1
                )
        dct['county'] = df_county
    return dct




        

def tester_ff10():

    fname = 'SmokeFlatFile_NONPOINT_20230330.csv'
    df = read_ff10(fname)

def tester_invtable():

    fname = 'invtables/invtable_2014platform_nointegrate_01may2019_v2.txt'
    df = read_invtable(fname)

def tester_sccdesc(lv=None):
    fname = '../../ge_dat/smkreport/sccdesc_2020platform_07apr2023_v0.txt'
    df = read_sccdesc(fname, lv)
    return df

def snoop_cmdline():
    import sys
    o = snoop_ff10(sys.argv[1])
    print(o)
def cmdline():
    import argparse, sys
    p = argparse.ArgumentParser(
            prog='smoke_reader',
            description='reads smoke files',
            )
    p.add_argument('filename')
    p.add_argument('-r', '--read', action=argparse.BooleanOptionalAction, help='read (or just snoop)')

    p.add_argument('-i', '--invtable', action=argparse.BooleanOptionalAction, help='read invtable')
    p.add_argument('-s', '--sccdesc', action=argparse.BooleanOptionalAction, help='read sccdesc')

    args = p.parse_args()

    if args.invtable:
        o = read_invtable(args.filename)
        print(o)
        return o

    if args.sccdesc:
        o = read_sccdesc(args.filename)
        print(o)
        return o

    if args.read:
        o = read_ff10(args.filename)
        print(o)
        return o
    else:
        o = snoop_ff10(args.filename)
        print(o)
        return o


if __name__ == '__main__':
    
    #df = tester_invtable()
    #snoop_cmdline()
    o = cmdline()



