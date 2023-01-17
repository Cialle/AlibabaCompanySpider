import requests
from bs4 import BeautifulSoup
import json
import csv
from multiprocessing.dummy import Pool
import pandas as pd


class AlibabaSearchSpider():

    def __init__(self):
        self.url = 'https://open-s.alibaba.com/openservice/mobileManufactoryViewService'

        self.headers = {
            'authority': 'open-s.alibaba.com',
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'zh-CN,zh-TW;q=0.9,zh;q=0.8,en;q=0.7,en-US;q=0.6', 
            'origin': 'https://m.alibaba.com',
            'referer': 'https://m.alibaba.com/',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-site',
            'user-agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 13_2_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.3 Mobile/15E148 Safari/604.1',
        }

        self.params = {
            'SearchText': '',
            'tab': 'supplier',
            'page': '1',
            'sourceFrom': 'wap',
            'Country': 'CN', 
        }

    def spiderSearch(self, keyword):
        """ 从搜索页面获取对应关键字的所有公司信息，作为二维列表返回

        :param keyword: 关键词

        return: [[companyId, companyUrl, companyName, companyGoldYear, companyTransactions, companyKeywod],[...]]:
            companyId: 公司ID
            companyUrl: 公司简介URL
            companyName: 公司名称
            companyGoldYear: 公司金牌年份
            companyTransactions: 公司交易额
            companyKeywod: 搜索关键字
        """

        companyDataList = []

        page = 0
        while True:
            page += 1
            print(f"正在爬取数据----正在进行关键词: {keyword}中第{page}页的数据爬取...")

            temp = self.getJsonOfSearch(keyword, page)
            if temp    == []: continue   # 如果temp为 []，则表示getJsonOfSearch()获取Json数据失败，跳过本次循环
            if temp[0] == 0 : break    # 如果num 为 0 ，则表示该关键词在当前页数已经无法获取到有效商家了，则退出循环
                
            # 数据处理
            companyDataJsonList = temp[1]['data']['body']['moduleList']

            for i in range(temp[0]):
                companyDataJson = companyDataJsonList[i].get('data', {})

                companyId = companyDataJson.get('companyId', '-')
                companyUrl = companyDataJson.get('action', '-')
                companyName = companyDataJson.get('companyName', '-')
                companyGoldYear = companyDataJson.get('goldYears', '-').replace(" yrs", "")
                companyTransactions = companyDataJson.get('transactions', '-').replace("US $", "").replace("+", "").replace(",", "").replace(".", "")
                


                companyKeywod = keyword

                companyDataList.append([companyId, companyUrl, companyName, companyGoldYear, companyTransactions, companyKeywod]) 
                print(f"正在爬取数据----成功获取关键词：{companyKeywod}下的的：{companyName}")

        print(f"正在爬取数据----关键词: {keyword}已经爬取完成，共有{len(companyDataList)}条数据")
        return companyDataList

    def saveData(self, path, data):
        """ 将data保存在path中

        :param path: 需要保存的路径
        :param data: 需要保存的数据列表
        """
        header = ('companyId', 'companyUrl', 'companyName', 'companyGoldYear', 'companyTransactions', 'companyKeywod')
        with open (path, "w+", newline = "") as file:
            saveCsv = csv.writer(file)
            saveCsv.writerow(header)
            for i in data:
                saveCsv.writerow(i)   

    def getJsonOfSearch(self, keyword, page):
        """ 从搜索页面获取Json数据

        :param keyword: 关键词
        :param page: 页数
        rtype: [] 
            获取成功返回: [num , page_json]:
                num: 搜索到的结果数量
                page_json: 页面的Json数据
            获取失败返回: []
        """
        
        # 设置爬取页面params
        self.params['page'] = page
        self.params['SearchText'] = keyword

        # 失败次数
        fail_Times = 0
        while True:
            try: 
                response = requests.get(url = self.url, headers = self.headers, params = self.params, timeout=(3.02,6.02)) # requests.get可能因网络原因无法打访问，导致报错
                page_Json = json.loads(response.text)
                num = page_Json['data']['body']['productNum'] # page_Json可能因服务器返回异常数据导致无法获取结果数量，导致报错
                # 如果没有报错，且获取成功结果数量，则返回相关数据
                return [num , page_Json]
            except:
                fail_Times += 1
                if fail_Times >= 5: # 连续5次获取失败，返回空列表
                    print(f"getJsonOfSearch----{keyword}----{page}----getJsonOfSearch()多次尝试仍错误，跳过该页面")
                    return []

                # 如果获取失败重新来。
                print(f"getJsonOfSearch----{keyword}----{page}----getJsonOfSearch()错误，正在重试")
       
    def spider(self,KeywordPath, savepath, poolnum):
        """ 以线程池的方式在搜索页面搜索关键词列表下的alibaba国际站商家数据，并保存到本地。

        :param keyword: 关键词路径，文件内每行一个关键字
        :param savepath: 保存路径
        :param poolnum: 线程池数量
        """

        content = []
        with open(KeywordPath, encoding='utf-8') as f:
            content = f.read().split("\n")
            content = [i for i in content if i != '']

        pool=Pool(poolnum)
        data_p = pool.map(self.spiderSearch, content)
        pool.close()
        pool.join()

        data = []
        for i in data_p:
            data += i

        self.saveData(savepath, data)

class ProcessData():
    def __init__(self):
        self.csv = ""
        self.df = ""

    def cheakTransactions(self, i):
        """ 查询当前第i行的companyTransactions是否存在，如不存在则刷新数据，用于类内部调用

        :param i: Panda的行数
        """
        if self.df.iat[i , 4] == '-':
            fail_Times = 0
            while True:
                headers = {
                    'authority': 'm.en.alibaba.com',
                    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                    'accept-language': 'zh-CN,zh-TW;q=0.9,zh;q=0.8,en;q=0.7,en-US;q=0.6',
                    'cache-control': 'max-age=0',
                    'sec-fetch-dest': 'document',
                    'sec-fetch-mode': 'navigate',
                    'sec-fetch-site': 'none',
                    'sec-fetch-user': '?1',
                    'upgrade-insecure-requests': '1',
                    'user-agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 13_2_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.3 Mobile/15E148 Safari/604.1',
                }
                try:
                    response = requests.get(url = self.df.iat[i , 1], headers = headers, timeout=(3.02,6.02))
                    soup = BeautifulSoup(response.text , 'lxml')
                    text = soup.select('div.block-transaction > div:nth-child(2) > div')
                    self.df.iat[i , 4] = text[0].text.replace("US $", "").replace("+", "").replace(",", "").replace(".", "")
                    print(f"正在刷新数据--------companyId:{self.df.iat[i , 0]}刷新数据成功~")
                    return

                except:
                    fail_Times += 1
                    if fail_Times >= 5: # 连续5次获取失败，返回空列表
                        print(f"正在刷新数据--------companyId:{self.df.iat[i , 0]}已经多次错误错误，跳过该页面----------------")
                        return

                    print(f"正在刷新数据--------companyId:{self.df.iat[i , 0]}错误，正在重试-----------------")

    def process(self, Path, poolnum):
        """ 以线程池的方式检查搜索到的alibaba会员站数据，因alibaba搜索接口得到的商家Transactions并不准确，如未搜到需深入商家页面重新搜索Transactions并刷新保存到本地。

        :param Path: 原始数据文件路径
        :param poolnum: 线程池数量
        """

        self.csv = pd.read_csv(Path,low_memory=False,error_bad_lines=False)#读取csv中的数据
        self.df = pd.DataFrame(self.csv).drop_duplicates(keep=False)

        pool=Pool(poolnum)
        pool.map(self.cheakTransactions, [i for i in range(self.df.shape[0])])
        self.df.to_csv(Path, index=False)
        print(f"处理完成，共{self.df.shape[0]}条数据")



def main():
    keyWordPath = "keyword.txt"
    savePath = "save.csv"

    # 通过搜索页面大概把数据爬下来，保存到savePath
    a = AlibabaSearchSpider()
    a.spider(keyWordPath, savePath, 8)
    
    # 检查savePath文件，将未成功爬取的数据进入商家页面重新爬取
    b = ProcessData()
    b.process(savePath, 8)


if __name__ == '__main__':
   main()