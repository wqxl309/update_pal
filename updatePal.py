# coding=utf8
import os
import time
import datetime as dt

import scipy.io as scio
import h5py
import pandas as pd
import numpy as np

from gmsdk import md
from WindPy import w




def read_cell(h5fl,field):
    return [''.join([chr(c) for c in h5fl[cl]]) for cl in h5fl[field][0]]

def updatePal(palPath=None):
    start = time.time()

    md.init('18201141877', 'Wqxl7309')
    if not w.isconnected():
        w.start()

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
    newDateNum = len(betweenDaysNumber)

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
        pd.DataFrame(np.transpose(idxArray)).to_csv(os.path.join(tempFilePath,'index_{}.csv'.format(idx)),index=False,header=False)

    # update stock info
    nCut = savedPal['N_cut'][0][0]    # 6000
    nEnd = savedPal['N_end'][0][0]    # last end date id ex.6732
    stockNames = read_cell(savedPal, 'stockname')
    savedStkcdsGM = ['.'.join([stk[-2:]+'SE',stk[:6]]) for stk in stockNames]
    savedStkNum = len(stockNames)
    listedStkcdsWind =  w.wset('sectorconstituent','date={};sectorid=a001010100000000'.format(availableDateStr)).Data[1]
    newStkcdsWind = sorted(list(set(listedStkcdsWind) - set(stockNames)))
    if newStkcdsWind:
        stockNames.extend( newStkcdsWind )
        newStkIpos = [int(tdt.strftime('%Y%m%d')) for tdt in w.wss(newStkcdsWind, 'ipo_date').Data[0]]
        newIpoIds = [(w.tdayscount(nextTrd,str(ipo)).Data[0][0]+nEnd) for ipo in newStkIpos]
        newStockip = pd.DataFrame([[int(newStkcdsWind[dumi][:6]), newStkIpos[dumi], newIpoIds[dumi],0,0,0,0,0] for dumi in range(len(newStkcdsWind))])
        newStockip.to_csv( os.path.join(tempFilePath,'stockip.csv'),index=False,header=False )
    else:
        pd.DataFrame([]).to_csv(os.path.join(tempFilePath, 'stockip.csv'), index=False, header=False)
    newStkcdsGm = ['.'.join([stk[-2:]+'SE',stk[:6]]) for stk in newStkcdsWind]
    allStkcdsGM = savedStkcdsGM + newStkcdsGm     # 全体股票包含已退市 与pal行数相同
    # allSecNames = pd.DataFrame(w.wss(stockNames,'sec_name').Data[0])
    allInstruments = md.get_instruments('SZSE', 1, 0) + md.get_instruments('SHSE', 1, 0)
    allInstrumentsDF = pd.DataFrame([[inds.symbol, inds.sec_name] for inds in allInstruments],columns=['symbol','sec_name']).set_index('symbol')
    allSecNames = allInstrumentsDF.loc[allStkcdsGM,'sec_name']
    allSecNames.to_csv( os.path.join(tempFilePath, 'sec_names.csv'), index=False, header=False )
    pd.DataFrame(newStkcdsWind).to_csv( os.path.join(tempFilePath, 'stockname.csv'), index=False, header=False )

    # update trade info
    pages = ['date','open','high','low','close','volume','amount','pctchg','flow_a_share','total_share','adjfct','adjprc','isst']
    newPal = {}
    for page in pages:
        newPal[page] = pd.DataFrame(np.zeros([len(allStkcdsGM), newDateNum]),index=allStkcdsGM,columns=betweenDays)
    lastPal = pd.DataFrame(savedPal['Pal'][:,-1,:],columns=savedStkcdsGM)
    barsDaily = md.get_dailybars(','.join(allStkcdsGM), nextTrdStr, availableDateStr)
    for bar in barsDaily:
        tdt = bar.strtime[:10]
        stk = '.'.join([bar.exchange,bar.sec_id])
        newPal['date'].loc[stk, tdt] = int(tdt.replace('-',''))
        newPal['open'].loc[stk, tdt] = bar.open
        newPal['high'].loc[stk, tdt] = bar.high
        newPal['low'].loc[stk, tdt] = bar.low
        newPal['close'].loc[stk, tdt] = bar.close
        newPal['volume'].loc[stk, tdt] = bar.volume
        newPal['amount'].loc[stk, tdt] = bar.amount
        newPal['pctchg'].loc[stk, tdt] = bar.close/bar.pre_close - 1
        # 计算自算复权因子 ： 前一日收盘价*(1+当日收益率)/当日收盘价 s.t. （当日收盘价*当日复权因子）/前一日收盘价 = 1+ret
        # 若当日没有交易 ： 沿用前一日 复权因子  循环外处理
        # 若前一日没有交易 前一日收盘价 特殊处理：
        #  当日有交易 ： 取停牌前最后一个交易日的 收盘价
        #  当日没交易 没有退市 ： 沿用前一日复权因子  循环外处理
        #  当日没交易 已经退市 ： 沿用前一日复权因子  循环外处理
        # 若新股上市第一天 ： 复权因子为1
        if stk in newStkcdsGm:
            newPal['adjfct'].loc[stk, tdt] = 1
        else:
            noTrdLast = (lastPal.loc[0, stk] == 0) if tdt == nextTrdStr else (newPal['date'].loc[stk, betweenDays[betweenDays.index(tdt) - 1]] == 0)
            if noTrdLast: # 前一日没交易 今日有交易（否则不应出现在bars里面）
                lastBar = md.get_last_n_dailybars(stk, 2, end_time=tdt)[-1]
                newPal['adjfct'].loc[stk, tdt] = lastPal.loc[15, stk] * lastBar.close * (1 + newPal['pctchg'].loc[stk, tdt]) / bar.close
            else:
                preClose = lastPal.loc[4,stk] if tdt==nextTrdStr else newPal['close'].loc[stk,betweenDays[betweenDays.index(tdt)-1]]
                newPal['adjfct'].loc[stk, tdt] = lastPal.loc[15, stk] * preClose * (1 + newPal['pctchg'].loc[stk, tdt]) / bar.close
    for dumi,tdt in enumerate(betweenDays):
        idx = newPal['adjfct'].loc[:,tdt]==0
        idx = idx.values
        if tdt==nextTrdStr:
            newPal['adjfct'].loc[idx[:savedStkNum], tdt] = lastPal.loc[15,:].values[idx[:savedStkNum]]
        else:
            newPal['adjfct'].loc[idx, tdt] = newPal['adjfct'].loc[idx, betweenDays[dumi-1]]
    newPal['adjprc'] = newPal['adjfct']*newPal['close']

    shareBar = md.get_share_index(','.join(allStkcdsGM), nextTrdStr, availableDateStr)
    for bar in shareBar:
        tdt = bar.pub_date
        stk = bar.symbol
        newPal['flow_a_share'].loc[stk, tdt] = bar.flow_a_share
        newPal['total_share'].loc[stk, tdt] = bar.total_share

    isST = np.array([int('ST' in sn) for sn in allSecNames.values])
    newPal['isst'] = pd.DataFrame(np.repeat(np.reshape(isST,(isST.shape[0],1)),len(betweenDays),axis=1), index=allStkcdsGM, columns=betweenDays)

    for page in newPal:
        newPal[page].to_csv(os.path.join(tempFilePath,'{}.csv'.format(page)),index=False,header=False )

    print('Pal temp files update finished with {0} stocks and {1} days in {2} seconds '.format(len(newStkcdsWind),len(betweenDays),time.time() - start))

if __name__=='__main__':
    updatePal()
