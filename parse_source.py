import asyncio
import random
import re
import csv
import time

from lxml import etree
import os
import json
from ichrome import AsyncChromeDaemon

SAVE_TITLE = []
headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36"
    }


def paser(info):
    """
    数据解析处理，主要功能是从网页中进行字段数据提取
    :param info:
    :return:
    """
    source = info.get("source").replace('""', '"')
    tree = etree.HTML(source)
    res = {
        "auction_id": "".join(re.findall("(\d+).htm", info.get("url", ""))),
        "url": info.get("url"),
        "auction_status": my_xpath(tree, "//div[contains(@class,'pm-main-l')]//h1/text()"),
        "province": info.get("province", ""),
        "city": info.get("city", ""),
        "district": my_xpath(tree, "//div[@id='itemAddress']/parent::*//text()"),
        "title": my_xpath(tree, "//div[contains(@class,'pm-main')]/h1/text()"),
        "address": my_xpath(tree, "//div[contains(@class,'pm-main')]/h1/text()"),
        "round": my_xpath(tree, "//div[contains(@class,'pm-main')]/h1/span[contains(@class,'item')]/text()"),
        "listing_time": my_xpath(tree, "//h1[contains(text(),'网络竞价成功确认书')]/following-sibling::*//p[contains(text(),'网拍公告时间')]//text()"),
        "start_time": my_xpath(tree, "//h1[contains(text(),'网络竞价成功确认书')]/following-sibling::*//p[contains(text(),'网拍开始时间')]//text()"),
        "end_time": my_xpath(tree, "//h1[contains(text(),'网络竞价成功确认书')]/following-sibling::*//p[contains(text(),'网拍结束时间')]//text()"),
        "end_time2": my_xpath(tree, "//span[contains(@class,'J_TimeLeft')]//text()"),
        "num_extensions": my_xpath(tree, "//*[contains(@id,'J_Delay')]//text()"),
        "num_bids": my_xpath(tree, "//a[contains(text(),'竞买记录')]//text() | //a[contains(text(),'应买记录')]//text() "),
        "enrollment": my_xpath(tree, "//div[contains(@class,'pm-main')]//span[contains(.,'人报名')]//text()"),
        "reminder": my_xpath(tree, "//div[contains(@class,'pm-main')]//span[contains(.,'人设置提醒')]//text()"),
        "views": my_xpath(tree, "//div[contains(@class,'pm-main')]//span[contains(.,'次围观')]//text()"),
        "current_price": my_xpath(tree, "//div[contains(@class,'pm-main')]//span[contains(.,'当前价')]/following-sibling::*//em/text()"),
        "deposit": my_xpath(tree, "//div[contains(@class,'pm-main')]//span[contains(.,'保证金')]/following-sibling::*[contains(@class,'price')]//text()"),
        "valuation": my_xpath(tree, "//div[contains(@class,'pm-main')]//span[contains(.,'市场价')]/following-sibling::*[contains(@class,'price')]//text() | //div[contains(@class,'pm-main')]//span[contains(.,'评 估 价')]/following-sibling::*[contains(@class,'price')]//text()"),
        "bid_duration": my_xpath(tree, "//div[contains(@class,'pm-main')]//span[contains(.,'竞价周期')]/following-sibling::*//text()|//div[contains(@class,'pm-main')]//span[contains(.,'变卖周期')]/following-sibling::*//text()"),
        "start_price": my_xpath(tree, "//div[contains(@class,'pm-main')]//span[contains(.,'起拍价')]/following-sibling::*//text() | //div[contains(@class,'pm-main')]//span[contains(.,'变卖价')]/following-sibling::*//text()"),
        "priority": "否" if "无" in my_xpath(tree, "//div[contains(@class,'pm-main')]//span[contains(@class,'pay-mark')][contains(.,'优先购买权人')]/following-sibling::*[not(@style)]//text()") else "是",
        "delay": my_xpath(tree, "//div[contains(@class,'pm-main')]//span[contains(.,'延时周期')]/following-sibling::*[not(@style)]//text()"),
        "markup": my_xpath(tree, "//div[contains(@class,'pm-main')]//span[contains(.,'加价幅度')]/following-sibling::*//text()"),
        "credit": my_xpath(tree,"//div[contains(@class,'pm-main')]//span[contains(.,'最高可赊')]//text()"),
        "credit_rate": "从credit字段中提取",
        "credit_max": "从credit字段中提取",
        "court": my_xpath(tree,"//span[contains(text(),'处置单位')]/following-sibling::span//text()"),
        "contact": my_xpath(tree,"//p[contains(.,'联系方式')]//following-sibling::div[contains(@class,'unit-txt ')]//span[contains(text(),'电话')]/following-sibling::span//text() | //p[contains(.,'联系方式')]//following-sibling::div[contains(@class,'unit-txt ')]//span[contains(text(),'手机')]/following-sibling::span//text()", join_str=" "),
        "service_provider": my_xpath(tree, "//p[contains(.,'联系方式')]//following-sibling::div[contains(@class,'unit-txt ')]/em/text()"),
        "auction_guarantee": "是" if len(my_xpath(tree,"//img[contains(@src,'6000000002909')]/@src")) >=1 else "否",
        "vr": "从vr_url进行提取",
        "vr_url": my_xpath(tree,"//img[contains(@class,'vrLinkImage')]/parent::a/@href"),
        "video": "从video_url进行提取",
        "video_url": my_xpath(tree,"//*[@id='J_video']/@data-src"),
        "num_pictures": "从picture_urls进行提取",
        "info_updates": len(my_xpath(tree, "//p/span[contains(text(),'进行了更新')]")),
        "picture_urls": tree.xpath("//ul[contains(@id,'J_UlThumb')]/li//a/img[not(contains(@src,'6000000003138'))]/@src"),
        "bidding": "直接从接口中进行获取",
        "attachments": standardization_file(tree),
        "house_size": standardization_regex("建筑面积: (\d+.*?\d+)平方米", source),
        "house_type": standardization_regex("房屋用途:(.*?)，", source),
        "land_type": my_xpath(tree,"//td[contains(.//text(),'土地用途')]/following-sibling::*//text()"),
        "land_duration": "",
        "land_size": standardization_regex("土地面积.*?([\d.]+)[㎡平方米]{1,3}", source),
        "structure": "",
        "product_status": my_xpath(tree,"//td[contains(.,'标的现状（包含租赁、占有、附随义务等）')]/following-sibling::td//text()"),
        "tax": "",
        "restriction": my_xpath(tree,"//td[contains(.,'权利限制情况')]/following-sibling::td//text()"),
    }
    standardization(res)


def standardization_file(tree):
    """
    标准化处理附件格式
    :param tree:
    :return:
    """
    files = []
    for a_tab in tree.xpath("//p[contains(@id,'J_DownLoad')]/a"):
        file_url = "".join(a_tab.xpath("./@href"))
        if not file_url.startswith("http"):
            file_url = "http:" + file_url
        file = {
            "file_name": "".join(a_tab.xpath(".//text()")),
            "file_url": file_url,
        }
        files.append(file)
    return json.dumps(files, ensure_ascii=False)


def standardization(info):
    """
    数据标准化
    :param info:
    :return:
    """
    global SAVE_TITLE
    end_time2 = info.pop("end_time2")
    if SAVE_TITLE == []:
        parse_title = [i for i in info.keys()] if SAVE_TITLE == [] else SAVE_TITLE
        standardization_title = []
        for tit in parse_title:
            standardization_title.append(tit)
            standardization_title.append("_{}".format(tit))
        SAVE_TITLE = parse_title
    for key in SAVE_TITLE:
        if key.startswith("_"):
            info[key] = info[key.replace("_",  "", 1)]

    if info.get("auction_status"):
        auction_status_lst = ["正在进行", "即将开始", "已结束", "中止", "撤回", "流拍"]
        now_auction_status = info.get("auction_status")
        for auction_status in auction_status_lst:
            if auction_status in now_auction_status:
                info["auction_status"] = auction_status
                break
        else:
            info["auction_status"] = ""
    if info.get("district"):
        res_district_lst = ["沧浪", "平江", "金阊", "虎丘", "吴中", "相城", "姑苏", "常熟", "张家港", "昆山", "吴江", "太仓", "苏州工业园", "新区", "园区"]
        now_district = info.get("district")
        res1 = re.findall("江苏省.*?苏州市\s*?([^- ]*?[区市])", now_district)
        res2 = re.findall("江苏省-苏州市[- ]{1}(\S+?[区市])", now_district)
        if len(res1) >= 1:
            info["district"] = res1[0]
        elif len(res2) >= 1:
            info["district"] = res2[0]
        else:
            info["district"] = ""
        if info["district"] != "":
            for district in res_district_lst:
                if district in info["district"]:
                    break
            else:
                info["district"] = ""
        if info["district"] == "":
            if "工业园区" in info.get("address", ""):
                info["district"] = "工业园区"
        if info["district"] == "苏州工业园区":
            info["district"] = "工业园区"
    if info.get("address"):
        address = info.get("address")
        while True:
            address = re.sub("^[（(][^（(）)]*?[)）]", "", address)
            address = re.sub("[（(]{1}[^（(）)]*?[)）]{1}$", "", address)
            address = re.sub("【.*】", "", address)
            address = re.sub(".*名下位于", "", address)
            address = re.sub(".*名下", "", address)
            address = re.sub("的不动产.*", "", address)
            address = re.sub("不动产.*", "", address)
            address = re.sub("的房产.*", "", address)
            address = re.sub("房产.*", "", address)
            address = re.sub("的房地产.*", "", address)
            address = re.sub("房地产.*", "", address)
            if address.endswith("）") or address.startswith("（"):
                continue
            else:
                break
        info["address"] = address.replace("坐落于", "").replace("位于", "")
    if info.get("round"):
        round_lst = ["一拍", "二拍", "变卖"]
        now_round = info.get("round")
        for auction_status in round_lst:
            if auction_status in now_round:
                info["round"] = auction_status
                break
        else:
            info["round"] = ""
    if info.get("listing_time"):
        info["listing_time"] = standardization_regex("[0-9]{4}.*", info.get("listing_time"))
    if info.get("start_time"):
        info["start_time"] = standardization_regex("[0-9]{4}.*", info.get("start_time"))
    if info.get("end_time"):
        info["end_time"] = standardization_regex("[0-9]{4}.*", info.get("end_time"))
    else:
        info["end_time"] = end_time2
    if info.get("num_extensions"):
        info["num_extensions"] = standardization_regex("\d+", info.get("num_extensions"))
    if info.get("num_bids"):
        info["num_bids"] = standardization_regex("\d+", info.get("num_bids"))
    if info.get("enrollment"):
        info["enrollment"] = standardization_regex("\d+", info.get("enrollment"))
    if info.get("reminder"):
        info["reminder"] = standardization_regex("\d+", info.get("reminder"))
    if info.get("views"):
        info["views"] = standardization_regex("\d+", info.get("views"))
    if info.get("current_price"):
        info["current_price"] = standardization_regex("[\d,.]+", info.get("current_price"))
    if info.get("deposit"):
        info["deposit"] = standardization_regex("[\d,.]+", info.get("deposit"))
    if info.get("valuation"):
        info["valuation"] = standardization_regex("[\d,.]+", info.get("valuation"))
    if info.get("bid_duration"):
        info["bid_duration"] = info.get("bid_duration").replace(": ", "")
    if info.get("start_price"):
        info["start_price"] = standardization_regex("[\d,.]+", info.get("start_price"))
    if info.get("delay"):
        info["delay"] = info.get("delay").replace(": ", "")
    if info.get("markup"):
        info["markup"] = standardization_regex("[\d,.]+", info.get("markup"))
    if info.get("credit"):
        try:
            info["credit_rate"] = int(standardization_regex("([\d]+)%", info.get("credit"))) / 100
        except:
            info["credit_rate"] = standardization_regex("([\d]+)%", info.get("credit"))
        info["credit_max"] = standardization_regex("([\d]+万)", info.get("credit"))
        info["credit"] = "是"
    else:
        info["credit_rate"] = ""
        info["credit_max"] = ""
        info["credit"] = "否"
    if info.get("vr_url"):
        info["vr"] = "是"
        if not info["vr_url"].startswith("http"):
            info["vr_url"] = "http:" + info["vr_url"]
    else:
        info["vr"] = "否"
    if info.get("video_url"):
        info["video"] = "是"
        if not info["video_url"].startswith("http"):
            info["video_url"] = "http:" + info["video_url"]
    else:
        info["video"] = "否"
    if info.get("picture_urls"):
        info["num_pictures"] = len(info.get("picture_urls"))
        picture_urls = []
        for url in info.get("picture_urls"):
            if not url.startswith("http"):
                url = "http:" + url
            url = url.replace("80x80", "960x960")
            picture_urls.append(url)
        info["picture_urls"] = json.dumps(picture_urls)
    else:
        info["picture_urls"] = json.dumps([])
    try:
        if int(info.get("num_bids")) != 0:
            info["bidding"] = dowload_bidding(info["auction_id"], [], int(info.get("num_bids")))
        else:
            info["bidding"] = []
    except:
        info["bidding"] = []

    save_data(info, file_name="../standardization.csv")


async def crawler_bidding(id, biddings, numbers):
    async with AsyncChromeDaemon(headless=True) as cd:
        async with cd.connect_tab() as tab:
            url = "https://sf-item.taobao.com/json/get_bid_records.htm?currentPage={}&_ksTS={}&id=%s&records_type=pageRecords" % (
            id,)
            t = str(int(time.time() * 1000)) + "_" + str(random.randint(100, 999))
            all_page = int(numbers /20) + 1
            for i in range(1, all_page+1):
                new_url = url.format(i, t)
                await tab.set_url(new_url)
                try:
                    parse = await tab.html
                    for data in re.findall("{.*?}", re.findall("records:.*?\[(.*?)\]",parse, re.DOTALL)[0], re.DOTALL):
                        res = []
                        res.append("出局" if re.findall('status:(.*),', data)[0] == "-1" else "成交")
                        res.append(re.findall('alias:"(.*?)"', data)[0])
                        res.append(re.findall('price:"(.*?)"', data)[0])
                        res.append(re.findall('date:"(.*?)"', data)[0])
                        biddings.append(res)
                except:
                    continue
            return biddings


def dowload_bidding(id, biddings, numbers):
    return asyncio.run(crawler_bidding(id, biddings, numbers))

def standardization_regex(regex, value):
    if value.strip() == "":
        return ""
    else:
        return "".join(re.findall(regex, value)).strip()

def my_xpath(tree: etree, xpath: str, join_str="") -> list:
    try:
        res = [i.replace(r"\r\n", "").replace(r"\n", "").strip() if isinstance(i, str) else i for i in tree.xpath(xpath)]
        try:
            res = f"{join_str}".join(res).strip()
        except Exception:
            pass
    except Exception:
        res = []
    return res


def save_data(result, file_name='parse.csv'):
    csv_title = [i for i in result.keys()] if SAVE_TITLE == [] else SAVE_TITLE
    if not os.path.exists(file_name):
        with open(file_name, "w", encoding="utf8", newline="\n") as fp:
            write = csv.writer(fp, delimiter=chr(9578))
            write.writerow(csv_title)
    savedata = list(map(lambda key: result.get(key), csv_title))
    with open(file_name, "a", encoding="utf8", newline="\n") as fp:
        write = csv.writer(fp, delimiter=chr(9578))
        write.writerow(savedata)


def read_csv(file_path):
    with open(file_path, 'r', encoding='utf-8') as fp:
        title = []
        line = 1
        for data in fp:
            if line == 1:
                title = data.strip().split(chr(9578))
            else:
                try:
                    info = data.split(chr(9578), len(title))
                    data_info = {title[i]: info[i] for i in range(len(title))}
                    data_info["source"] = '""' + data_info["source"].strip() + '""'
                    paser(data_info)
                    print(f"line: {line} 数据解析成功")
                except Exception as e:
                    print(f"line: {line} 数据解析错误：{e}")
            line += 1


def run(file_path):
    read_csv(file_path)


if __name__ == '__main__':
    run("source.csv")

