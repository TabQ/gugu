# -*- coding:utf-8 -*-
"""
股票信息类
Created on 2019/01/02
@author: TabQ
@group : gugu
@contact: 16621596@qq.com
"""
from __future__ import division

import pandas as pd
from pandas.compat import StringIO
import json
import lxml.html
from lxml import etree
import random
import re
import time
from gugu.utility import Utility
from gugu.base import Base, cf
import sys

ua_list = [
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/30.0.1599.101',
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.122',
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.71',
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95',
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.71',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36',
    'Mozilla/5.0 (Windows NT 5.1; U; en; rv:1.8.1) Gecko/20061208 Firefox/2.0.0 Opera 9.50',
    'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:34.0) Gecko/20100101 Firefox/34.0',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:69.0) Gecko/20100101 Firefox/69.0',
]
headers = {
    'Accept': '*/*',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'zh-CN,zh;q=0.9',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': random.choice(ua_list),
    'Cache-Control': 'max-age=0',
}

class StockInfo(Base):
    def stockProfiles(self):
        """
        获取上市公司基于基本面的汇总数据信息
        Return
        --------
        DataFrame or List: [{'symbol':, 'net_profit_cagr':, ...}, ...]
                   symbol:                  代码
                   net_profit_cagr:         净利润复合年均增长率
                   ps:                      市销率
                   percent：                涨幅
                   pb_ttm：                 滚动市净率
                   float_shares：           流通股本
                   current：                当前价格
                   amplitude：              振幅
                   pcf：                    市现率
                   current_year_percent：   今年涨幅
                   float_market_capital：   流通市值
                   market_capital：         总市值
                   dividend_yield：         股息率
                   roe_ttm：                滚动净资产收益率
                   total_percent：          总涨幅
                   income_cagr：            收益复合年均增长率
                   amount：                 成交额
                   chg：                    涨跌点数
                   issue_date_ts：          发行日unix时间戳
                   main_net_inflows：       主营净收入
                   volume：                 成交量
                   volume_ratio：           量比
                   pb:                      市净率
                   followers：              雪球网关注人数
                   turnover_rate：          换手率
                   name：                   名称
                   pe_ttm：                 滚动市盈率
                   total_shares：           总股本
        """
        self._data = pd.DataFrame()
        
        self._writeHead()
        
        self._data = self.__handleStockProfiles()
        self._data['issue_date_ts'] = self._data['issue_date_ts'].map(lambda x: int(x/1000))
        
        return self._result()

    def __handleStockProfiles(self):
        try:
            request = self._session.get(cf.XQ_HOME, headers=headers)
            cookies = request.cookies
        except Exception as e:
            print(str(e))

        page = 1
        while True:
            self._writeConsole()

            try:
                timestamp = int(time.time()*1000)
                request = self._session.get(cf.XQ_STOCK_PROFILES_URL % (page, timestamp), headers=headers, cookies=cookies)

                dataDict = json.loads(request.text)
                if not dataDict.get('data').get('list'):
                    break

                dataList = []
                for row in dataDict.get('data').get('list'):
                    dataList.append(row)

                self._data = self._data.append(pd.DataFrame(dataList, columns=cf.XQ_STOCK_PROFILES_COLS), ignore_index=True)

                page += 1
                time.sleep(1)
            except Exception as e:
                print(str(e))

        return self._data

    def report(self, year, quarter, retry=3, pause=0.001):
        """
        获取业绩报表数据
        Parameters
        --------
        year:int 年度 e.g:2014
        quarter:int 季度 :1、2、3、4，只能输入这4个季度
           说明：由于是从网站获取的数据，需要一页页抓取，速度取决于您当前网络速度
        retry : int, 默认 3
                     如遇网络等问题重复执行的次数 
        pause : int, 默认 0.001
                    重复请求数据过程中暂停的秒数，防止请求间隔时间太短出现的问题   
        Return
        --------
        DataFrame or List: [{'code':, 'name':, ...}, ...]
            code,代码
            name,名称
            eps,每股收益
            eps_yoy,每股收益同比(%)
            bvps,每股净资产
            roe,净资产收益率(%)
            epcf,每股现金流量(元)
            net_profits,净利润(万元)
            profits_yoy,净利润同比(%)
            distrib,分配方案
            report_date,发布日期
        """
        self._data = pd.DataFrame()
        
        if Utility.checkQuarter(year, quarter) is True:
            self._writeHead()
            
            # http://vip.stock.finance.sina.com.cn/q/go.php/vFinanceAnalyze/kind/mainindex/index.phtml?s_i=&s_a=&s_c=&reportdate=2018&quarter=3&p=1&num=60
            self._data = self.__parsePage(cf.REPORT_URL, year, quarter, 1, cf.REPORT_COLS, pd.DataFrame(), retry, pause, 11)
            if self._data is not None:
                self._data['code'] = self._data['code'].map(lambda x:str(x).zfill(6))
                
            return self._result()
        
    def profit(self, year, quarter, retry=3, pause=0.001):
        """
        获取盈利能力数据
        Parameters
        --------
        year:int 年度 e.g:2014
        quarter:int 季度 :1、2、3、4，只能输入这4个季度
           说明：由于是从网站获取的数据，需要一页页抓取，速度取决于您当前网络速度
        retry : int, 默认 3
                     如遇网络等问题重复执行的次数 
        pause : int, 默认 0.001
                    重复请求数据过程中暂停的秒数，防止请求间隔时间太短出现的问题   
        Return
        --------
        DataFrame or List: [{'code':, 'name':, ...}, ...]
            code,代码
            name,名称
            roe,净资产收益率(%)
            net_profit_ratio,净利率(%)
            gross_profit_rate,毛利率(%)
            net_profits,净利润(万元)
            eps,每股收益
            business_income,营业收入(百万元)
            bips,每股主营业务收入(元)
        """
        self._data = pd.DataFrame()
        
        if Utility.checkQuarter(year, quarter) is True:
            self._writeHead()
            
            # http://vip.stock.finance.sina.com.cn/q/go.php/vFinanceAnalyze/kind/profit/index.phtml?s_i=&s_a=&s_c=&reportdate=2018&quarter=3&p=1&num=60
            self._data = self.__parsePage(cf.PROFIT_URL, year, quarter, 1, cf.PROFIT_COLS, pd.DataFrame(), retry, pause)
            if self._data is not None:
                self._data['code'] = self._data['code'].map(lambda x: str(x).zfill(6))
                
            return self._result()
        
    def operation(self, year, quarter, retry=3, pause=0.001):
        """
        获取营运能力数据
        Parameters
        --------
        year:int 年度 e.g:2014
        quarter:int 季度 :1、2、3、4，只能输入这4个季度
           说明：由于是从网站获取的数据，需要一页页抓取，速度取决于您当前网络速度
        retry : int, 默认 3
                     如遇网络等问题重复执行的次数 
        pause : int, 默认 0.001
                    重复请求数据过程中暂停的秒数，防止请求间隔时间太短出现的问题   
        Return
        --------
        DataFrame or List: [{'code':, 'name':, ...}, ...]
            code,代码
            name,名称
            arturnover,应收账款周转率(次)
            arturndays,应收账款周转天数(天)
            inventory_turnover,存货周转率(次)
            inventory_days,存货周转天数(天)
            currentasset_turnover,流动资产周转率(次)
            currentasset_days,流动资产周转天数(天)
        """
        self._data = pd.DataFrame()
        
        if Utility.checkQuarter(year, quarter) is True:
            self._writeHead()
            
            # http://vip.stock.finance.sina.com.cn/q/go.php/vFinanceAnalyze/kind/operation/index.phtml?s_i=&s_a=&s_c=&reportdate=2018&quarter=3&p=1&num=60
            self._data = self.__parsePage(cf.OPERATION_URL, year, quarter, 1, cf.OPERATION_COLS, pd.DataFrame(), retry, pause)
            if self._data is not None:
                self._data['code'] = self._data['code'].map(lambda x: str(x).zfill(6))
                
            return self._result()
        
    def growth(self, year, quarter, retry=3, pause=0.001):
        """
        获取成长能力数据
        Parameters
        --------
        year:int 年度 e.g:2014
        quarter:int 季度 :1、2、3、4，只能输入这4个季度
           说明：由于是从网站获取的数据，需要一页页抓取，速度取决于您当前网络速度
        retry : int, 默认 3
                     如遇网络等问题重复执行的次数 
        pause : int, 默认 0.001
                    重复请求数据过程中暂停的秒数，防止请求间隔时间太短出现的问题   
        Return
        --------
        DataFrame or List: [{'code':, 'name':, ...}, ...]
            code,代码
            name,名称
            mbrg,主营业务收入增长率(%)
            nprg,净利润增长率(%)
            nav,净资产增长率
            targ,总资产增长率
            epsg,每股收益增长率
            seg,股东权益增长率
        """
        self._data = pd.DataFrame()
        
        if Utility.checkQuarter(year, quarter) is True:
            self._writeHead()
            
            # http://vip.stock.finance.sina.com.cn/q/go.php/vFinanceAnalyze/kind/grow/index.phtml?s_i=&s_a=&s_c=&reportdate=2018&quarter=3&p=1&num=60
            self._data = self.__parsePage(cf.GROWTH_URL, year, quarter, 1, cf.GROWTH_COLS, pd.DataFrame(), retry, pause)
            if self._data is not None:
                self._data['code'] = self._data['code'].map(lambda x: str(x).zfill(6))
                 
            return self._result()
        
    def debtPaying(self, year, quarter, retry=3, pause=0.001):
        """
        获取偿债能力数据
        Parameters
        --------
        year:int 年度 e.g:2014
        quarter:int 季度 :1、2、3、4，只能输入这4个季度
           说明：由于是从网站获取的数据，需要一页页抓取，速度取决于您当前网络速度
        retry : int, 默认 3
                     如遇网络等问题重复执行的次数 
        pause : int, 默认 0.001
                    重复请求数据过程中暂停的秒数，防止请求间隔时间太短出现的问题   
        Return
        --------
        DataFrame or List: [{'code':, 'name':, ...}, ...]
            code,代码
            name,名称
            currentratio,流动比率
            quickratio,速动比率
            cashratio,现金比率
            icratio,利息支付倍数
            sheqratio,股东权益比率
            adratio,股东权益增长率
        """
        self._data = pd.DataFrame()
        
        if Utility.checkQuarter(year, quarter) is True:
            self._writeHead()
            
            # http://vip.stock.finance.sina.com.cn/q/go.php/vFinanceAnalyze/kind/debtpaying/index.phtml?s_i=&s_a=&s_c=&reportdate=2018&quarter=3&p=1&num=60
            self._data = self.__parsePage(cf.DEBTPAYING_URL, year, quarter, 1, cf.DEBTPAYING_COLS, pd.DataFrame(), retry, pause)
            if self._data is not None:
                self._data['code'] = self._data['code'].map(lambda x: str(x).zfill(6))
                
            return self._result()
        
    def cashFlow(self, year, quarter, retry=3, pause=0.001):
        """
        获取现金流量数据
        Parameters
        --------
        year:int 年度 e.g:2014
        quarter:int 季度 :1、2、3、4，只能输入这4个季度
           说明：由于是从网站获取的数据，需要一页页抓取，速度取决于您当前网络速度
        retry : int, 默认 3
                     如遇网络等问题重复执行的次数 
        pause : int, 默认 0.001
                    重复请求数据过程中暂停的秒数，防止请求间隔时间太短出现的问题   
        Return
        --------
        DataFrame or List: [{'code':, 'name':, ...}, ...]
            code,代码
            name,名称
            cf_sales,经营现金净流量对销售收入比率
            rateofreturn,资产的经营现金流量回报率
            cf_nm,经营现金净流量与净利润的比率
            cf_liabilities,经营现金净流量对负债比率
            cashflowratio,现金流量比率
        """
        self._data = pd.DataFrame()
        
        if Utility.checkQuarter(year, quarter) is True:
            self._writeHead()
            
            # http://vip.stock.finance.sina.com.cn/q/go.php/vFinanceAnalyze/kind/cashflow/index.phtml?s_i=&s_a=&s_c=&reportdate=2018&quarter=3&p=1&num=60
            self._data = self.__parsePage(cf.CASHFLOW_URL, year, quarter, 1, cf.CASHFLOW_COLS, pd.DataFrame(), retry, pause)
            if self._data is not None:
                self._data['code'] = self._data['code'].map(lambda x: str(x).zfill(6))
                
            return self._result()
        
    def __parsePage(self, url, year, quarter, page, column, dataArr, retry, pause, drop_column=None):
        self._writeConsole()
        
        for _ in range(retry):
            time.sleep(pause)
            
            try:
                request = self._session.get( url % (year, quarter, page, cf.PAGE_NUM[1]), timeout=10 )
                request.encoding = 'gbk'
                text = request.text.replace('--', '')
                html = lxml.html.parse(StringIO(text))
                res = html.xpath("//table[@class=\"list_table\"]/tr")
                if self._PY3:
                    sarr = [etree.tostring(node).decode('utf-8') for node in res]
                else:
                    sarr = [etree.tostring(node) for node in res]
                sarr = ''.join(sarr)
                sarr = '<table>%s</table>'%sarr
                df = pd.read_html(sarr)[0]
                if drop_column is not None:
                    df = df.drop(drop_column, axis=1)
                df.columns = column
                dataArr = dataArr.append(df, ignore_index=True)
                nextPage = html.xpath('//div[@class=\"pages\"]/a[last()]/@onclick')
                if len(nextPage) > 0:
                    page = re.findall(r'\d+', nextPage[0])[0]
                    return self.__parsePage(url, year, quarter, page, column, dataArr, retry, pause, drop_column)
                else:
                    return dataArr
            except Exception as e:
                print(e)

        raise IOError(cf.NETWORK_URL_ERROR_MSG)