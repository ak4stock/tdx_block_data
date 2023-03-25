## -*- coding: utf-8 -*-

import pandas as pd
from struct import unpack

'''
通达信对不同的板块更新的频率不同，每天登录通达信行情软件，
自动会更新本地数据库文件，在上午9：00之后登录通达信行情软件就会自动更新。
'''

# 本地通达信安装路径
PATH = 'C:/tdx/T0002/hq_cache/'

def get_block_file(block='gn'):
    """ file= block_gn.dat, _fg.dat, _zs.dat  """
    if block == 'hy':
        return hy_block('hy')
    
    file_name = f'block_{block}.dat'
    #print(PATH + file_name)
    with open(PATH + file_name, 'rb') as f:
        buff = f.read()

    head = unpack('<384sh', buff[:386])
    blk = buff[386:]
    blocks = [blk[i * 2813:(i + 1) * 2813] for i in range(head[1])]
    bk_list = []
    for bk in blocks:
        name = bk[:8].decode('gbk').strip('\x00')
        num, t = unpack('<2h', bk[9:13])
        stks = bk[13:(12 + 7 * num)].decode('gbk').split('\x00')
        bk_list = bk_list + [[name, block, num, stks]]
    df = pd.DataFrame(bk_list, columns=['name', 'tp', 'num', 'stocks'])
    df.to_csv(file_name + '.csv')
    return df


def read_file_loc(file_name, splits):
    with open(file_name, 'r') as f:
        buf_lis = f.read().split('\n')
    return [x.split(splits) for x in buf_lis[:-1]]

def get_block_zs_tdx_loc(block='hy'):
    file = PATH + 'tdxzs3.cfg'
    buf_line = read_file_loc(file, '|')
    mapping = {'hy': '2', 'dq': '3', 'gn': '4', 'fg': '5', 'yjhy': '12', 'zs': '6'}
    df = pd.DataFrame(buf_line, columns=['name', 'code', 'type', 't1', 't2', 'block'])
    dg = df.groupby(by='type')
    if (block == 'zs'):
        return df
    temp = dg.get_group(mapping[block]).reset_index(drop=True)
    temp.drop(temp.columns[[2, 3, 4]], axis=1, inplace=True)
    return temp

def get_stock_hyblock_tdx_loc():
    file = PATH + 'tdxhy.cfg'
    buf_line = read_file_loc(file, '|')
    buf_lis = []
    mapping = {'0': 'sz.', '1': 'sh.', '2': 'bj.'}
    for x in buf_line:
        # x[1] = mapping[x[0]] + x[1]
        buf_lis.append(x)
    df = pd.DataFrame(buf_lis, columns=['c0', 'code', 'block', 'c1', 'c2', 'c3'])
    df.drop(df.columns[[0, 3, 4, 5]], axis=1, inplace=True)
    df = df[(df['block'] != '')]
    df['block5'] = df['block'].str[0:5]
    return df

def hy_block(blk='hy'):
    stocklist = get_stock_hyblock_tdx_loc()
    blocklist = get_block_zs_tdx_loc(blk)
    blocklist['block5'] = blocklist['block'].str[0:5]
    blocklist['num'] = 0
    blocklist['stocks'] = ''
    for i in range(len(blocklist)):
        blockkey = blocklist.iat[i, 2]
        if (len(blockkey) == 5):
            datai = stocklist[stocklist['block5'] == blockkey]  # 根据板块名称过滤
        else:
            datai = stocklist[stocklist['block'] == blockkey]  # 根据板块名称过滤
        # 板块内进行排序填序号
        datai = datai.sort_values(by=['code'], ascending=[True])
        codelist = datai['code'].tolist()
        blocklist.iat[i, 4] = len(codelist)
        blocklist.iat[i, 5] = str(codelist)
    df = blocklist.drop(blocklist[blocklist['num'] == 0].index)
    df.to_csv('block_hy.dat.csv', index=False)
    return df


if __name__ == "__main__":
    # 最后把文件写入本地 block_xx.dat.csv
    get_block_file('hy') 
    get_block_file('gn')
    get_block_file('zs')
    get_block_file('fg')
