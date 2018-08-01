# coding=utf8
import os
import datetime as dt

import scipy.io as scio
import h5py
import pandas as pd
import numpy as np

from gmsdk import md


md.init('18201141877','Wqxl7309')


def read_cell(h5fl,field):
    return [''.join([chr(c) for c in h5fl[cl]]) for cl in h5fl[field][0]]

def updatePal(palPath=None):

    palPath = r'E:\bqfcts\bqfcts\data\Paltest' if palPath is None else palPath
    tempFilePath = os.path.join(palPath,'temp_files')
    if not os.path.exists(tempFilePath):
        os.mkdir(tempFilePath)
    matName = 'data_20150701_now.mat'

    savedPal = h5py.File(os.path.join(palPath,matName))
    # print(read_cell(savedPal,'sec_names'))

    nextTrd = dt.datetime.strptime(str(int(savedPal['nexttrd'][0][0])),'%Y%m%d')
    nextTrdStr = nextTrd.strftime('%Y-%m-%d')
    updateTime = dt.datetime(nextTrd.year,nextTrd.month,nextTrd.day,15,30,0)
    if updateTime > dt.datetime.now():
        print('not update time yet')
        return
    else:
        availableDateStr = md.get_last_dailybars('SHSE.000001')[0].strtime[:10]
        if int(availableDateStr.replace('-','')) <= int(nextTrdStr.replace('-','')):
            print('new data not avaliable yet')
            return
        else:
            print('will update from {0} to {1}'.format(nextTrdStr,availableDateStr))

    betweenDays = [tdt.strtime[:10] for tdt in md.get_calendar('SHSE',nextTrdStr,availableDateStr)]
    if nextTrdStr!=availableDateStr:    # 避免同一日期重复
        betweenDays.append(availableDateStr)
    betweenDaysNumber = [int(tdt.replace('-','')) for tdt in betweenDays]
    print(betweenDays)

    # 更新前 先备份数据
    backupPath = os.path.join(palPath,'backup')
    cpResult = os.system(r'COPY {0} {1} /Y'.format(os.path.join(palPath,matName),os.path.join(backupPath,matName)))
    assert cpResult==0,'backup failed'

    gmDateFmt = 'yyyy-mm-dd'

    # update indice
    indiceNames = ['sh','hs300','zz500','sz50']
    indiceCodes = ['000001','000300','000905','000016']
    symbols = ','.join(['SHSE.{}'.format(sbl) for sbl in indiceCodes])
    indiceBars = md.get_dailybars(symbols,nextTrdStr,availableDateStr)
    for dumi,idx in enumerate(indiceNames):
        bars = indiceBars[dumi::4]
        idxret = np.array([bar.close for bar in bars])/np.array([bar.pre_close for bar in bars]) - 1
        idxArray = np.array([betweenDaysNumber,
                             [bar.open for bar in bars],
                             [bar.high for bar in bars],
                             [bar.low for bar in bars],
                             [bar.close for bar in bars],
                             [bar.volume for bar in bars],
                             [bar.amount for bar in bars],
                             idxret
                             ])
        # newIndex = np.column_stack([savedPal['index_{}'.format(idx)][:], idxArray])
        pd.DataFrame(np.transpose(idxArray)).to_csv(os.path.join(tempFilePath,'new_indice_{}.csv'.format(idx)),index=False,header=False)

    # update stock info
    notA = [stk[0] for stk in pd.read_csv(r'E:\bqfcts\bqfcts\data\Paltest\notA.csv').values]
    savedStkcds = ['.'.join([s[-2:]+'SE',s[:6]]) for s in read_cell(savedPal, 'stockname')]
    allStkcds = md.get_instruments('SZSE', 1, 0) + md.get_instruments('SHSE', 1, 0)
    newStkcds = [stk.symbol for stk in allStkcds if (stk.symbol not in notA) and (stk.symbol not in allStkcds)]




if __name__=='__main__':
    # updatePal()

    t = pd.read_csv(r'E:\bqfcts\bqfcts\data\Paltest\notA.csv').values
    print(t)

    # t = h5py.File(r'E:\bqfcts\bqfcts\data\Pal\data_20150701_now.mat')
    #
    # a = read_cell(t, 'stockname')
    # a = set(['.'.join([s[-2:]+'SE',s[:6]]) for s in a])
    # print(a)
    #
    # t1 = md.get_instruments('SZSE',1,0)
    # t2 = md.get_instruments('SHSE', 1, 0)
    #
    # print( len(t1) + len(t2) )
    # dif = (set([s.symbol for s in t1]) | set([s.symbol for s in t2])) - a
    # with open('miss.csv','w') as f:
    #     for k in dif:
    #         f.writelines(k)
    #         f.writelines('\n')
    # print(len(dif))

