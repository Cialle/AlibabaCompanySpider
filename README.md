# AlibabaCompanySpider

### 简介：
用搜索的方式爬取阿里巴巴国际站企业信息，用于经济学数据分析。

### 信息包含：
- 企业ID
- 企业链接
- 企业名称
- 企业金牌服务年限
- 企业交易量（如隐藏或抓取失败 用"-"替代）
- 搜索用的关键字

### 使用说明：
- keywords.txt：
  - 每行一个关键字，留空会忽略
  - 可输入城市名/品类名，例如Beijing/Lighters等，请自行发掘，只要能在https://m.alibaba.com/trade/search页面搜索的，自动搜索中国相关企业
- 结果自动保存在save.csv
- 请自行安装支持库
