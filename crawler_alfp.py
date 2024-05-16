import os
import time
import random
from ichrome import AsyncChromeDaemon
import asyncio
from lxml import etree
import shutil
import redis
import json
import csv
import aircv as ac
import win32con, win32gui, win32ui
from pynput.mouse import Button, Controller as c1
import pandas as pd

Chrome_Log_Path = "ichrome_log"


class Spider(object):
    def __init__(self):
        # redis主要用来存储url
        self.url_list_s = list()
        self.number = 0
        self.db_name = "alpm_res"
        self.redis_pool = redis.ConnectionPool(host='127.0.0.1', port=6379, db=8)
        self.client = redis.Redis(connection_pool=self.redis_pool)
        #self.client.delete(self.db_name)  # 清空之前数据
        self.start_url = "https://sf.taobao.com/"  # 这个url一定不要动，要不然会出现登录页面无法登录的界面

        self.page_list_url = "https://sf.taobao.com/list/50025969_____%CB%D5%D6%DD.htm?auction_source=0&st_param=-1&auction_start_seg=-1&auctionStartFrom=2020-01-01&auctionStartTo=2021-06-30&page={}"#换状态、时间切片、
        #爬取对应的网页
        #https://sf.taobao.com/list/50025969__2___%CB%D5%D6%DD.htm?auction_source=0&st_param=-1&auction_start_seg=0&auction_start_from=2021-01-01&auction_start_to=2022-03-31&page={}
        #设置时间切片（5个）
        self.proxy = None  # 如果有代理请在这里设置代理
        self.await_set_url_timeout = 2 # 设置每次请求的等待时间时间
        self.province = "浙江"  # 用于表明数据采集的是哪个省
        self.city = "杭州"  # 用于表明数据采集的是哪个市
        self.max_page = 92 # 用于采集的最大页数;防止超过一定量时浏览器内存溢出；

    async def wait_login(self, tab):
        """
        进入登录页面然后手动登录
        :param tab:
        :return:
        """
        await tab.set_url(self.start_url)
        await asyncio.sleep(self.await_set_url_timeout)
        # 在此处做点击是为了进入正常的登录界面
        await tab.click("div.list-box.type-list > ul > li:nth-child(2) > a")
        print("请在30s之内完成登录操作,如果没有登录界面，等待30s即可")
        await asyncio.sleep(30)
        if not await tab.wait_tag("div.sf-filter-value > ul > li:nth-child(1) > em > a", max_wait_time=5):
            print("未在规定时间内完成登录，请重新进行执行程序")
            return False
        else:
            print("登入成功，开始抓取列表页url")
            return True

    async def run_spider(self, crawler_list=True, start_page=1):
        """
        执行采集程序，采集程序分为两步，第一步采集列表页，第二次执行进行详情页采集
        :param crawler_list: 用于区分采集的是详情页还是列表页
        :param start_page: 只有采集列表页时才会生效，用于指定起始的采集页数
        :return: True or page， 如果返回True代表采集完成
        """
        async with AsyncChromeDaemon(proxy=self.proxy, port=9444,user_data_dir=Chrome_Log_Path) as cd:
            async with cd.connect_tab() as tab:
                if crawler_list:
                    if not await self.wait_login(tab):
                        return start_page
                    await asyncio.sleep(self.await_set_url_timeout)
                    while True:
                        await tab.set_url(self.page_list_url.format(start_page))
                        await asyncio.sleep(self.await_set_url_timeout)
                        if not await self.handle_slider(tab):
                            return start_page
                        else:
                            for url in etree.HTML(await tab.html).xpath("//ul[contains(@class,'item-list')]/li/a/@href"):
                                if url.split('.htm')[0] in self.url_list_s:
                                    print('内容值重复！')  #链接去重
                                    continue

                                data = {
                                    "url": "https:" + url,
                                    "province": self.province,
                                    "city": self.city
                                }
                                self.client.rpush(self.db_name, json.dumps(data, ensure_ascii=False))
                                print("插入:", data)
                                self.url_list_s.append(url.split('.htm')[0])
                                self.save_list(data)
                        start_page += 1
                        time.sleep(2)
                        if start_page > self.max_page:
                            return True
                        # save list here


                else:
                    listdata = pd.read_excel("hangzhou.xlsx")
                    while True:

                        listdata2 = listdata.iloc[-1]
                        print(listdata2)
                        listdata.drop([len(listdata)-1],inplace=True)
                        if listdata.empty:
                            return True
                        else:
                            url = listdata2["url"]
                            if "?track_id" in url:
                                url = url.split("?track_id")[0]
                            city = listdata2["city"]
                            province = listdata2["province"]
                            await tab.set_url(url)
                            await asyncio.sleep(self.await_set_url_timeout)
                            # if not await self.handle_slider(tab):
                            #     self.client.rpush(self.db_name, json.dumps(url_info, ensure_ascii=False))
                            #     return start_page
                            for i in range(30):
                                js = "document.documentElement.scrollTop = {}".format((i+1) * 800) #需要调整下滑距离，进行时间爬取
                                await tab.js(js)
                                await asyncio.sleep(0.4)
                            await asyncio.sleep(1)
                            #time.sleep(1)
                            res = {
                                "province": province,
                                "city": city,
                                "url": url,
                                "source": repr(await tab.html),
                            }
                            self.save_source(url, await tab.html)
                            self.save_csv(res)


                        # url_info = self.client.rpop(self.db_name)  # 删除尾端最后一行
                        # # print('*-**-*-*-*-*-*-*-*-*-*-*')
                        # print(url_info)
                        # if url_info is None:
                        #     return True
                        # else:
                        #     url_info = json.loads(url_info)
                        #     url = url_info.get("url")
                        #     if "?track_id" in url:
                        #         url = url.split("?track_id")[0]
                        #     city = url_info.get("city")
                        #     province = url_info.get("province")
                        #     await tab.set_url(url)
                        #     await asyncio.sleep(self.await_set_url_timeout)
                        #     if not await self.handle_slider(tab):
                        #         self.client.rpush(self.db_name, json.dumps(url_info, ensure_ascii=False))
                        #         return start_page
                        #     for i in range(30):
                        #         js = "document.documentElement.scrollTop = {}".format((i+1) * 800) #需要调整下滑距离，进行时间爬取
                        #         await tab.js(js)
                        #         await asyncio.sleep(0.4)
                        #     await asyncio.sleep(1)
                        #     #time.sleep(1)
                        #     res = {
                        #         "province": province,
                        #         "city": city,
                        #         "url": url,
                        #         "source": repr(await tab.html),
                        #     }
                        #     self.save_source(url, await tab.html)
                        #     self.save_csv(res)
                        #     #o = self.save_csv(res)
                        #     #if o == 0:  # 判断保存的个数到达数量，则关闭程序
                        #     #    return False

    async def handle_slider(self, tab):
        """
        校验
        :param tab:
        :return:
        """
        url = await tab.url
        if "login" in url:
            print("跳转到登录界面")
            return False
        for i in range(5):
            title = await tab.title
            if "验证码" in title:
                print("发现验证码，开始处理验证码")
                # 1. 获取窗口句柄
                hwnd = win32gui.FindWindow(0, "验证码拦截 - Google Chrome")
                # 2. 显示窗口
                self.show_chrome_top(hwnd)
                # 3. 保存游览器图片
                self.save_chrome_image(hwnd, img_name='output.png')
                # 4. 获取滑块的坐标
                coordinate = self.get_box_coordinate('output.png', 'span.png')
                if coordinate is not None:
                    left, top, right, bot = win32gui.GetWindowRect(hwnd)
                    mouse = c1()
                    x_coordinate = left + coordinate[0]
                    y_coordinate = top + coordinate[1]
                    # 5. 开始模拟滑动
                    mouse.position = (x_coordinate, y_coordinate)
                    mouse.press(Button.left)
                    await asyncio.sleep(1 + random.random())
                    trace = self.get_trace()
                    for d in trace:
                        mouse.move(d, 0)
                        await asyncio.sleep(random.random() / 10)
                    await asyncio.sleep(random.random())
                    mouse.release(Button.left)
                    await asyncio.sleep(self.await_set_url_timeout)
                    title = await tab.title
                    if "点击框体重试" in (await tab.html):
                        mouse.position = (x_coordinate+5, y_coordinate)
                        mouse.press(Button.left)
                        await asyncio.sleep(random.random() / 10)
                        mouse.release(Button.left)
                        await asyncio.sleep(1)
                if "验证码" in title:
                    print("验证码处理失败,重试")
                else:
                    return True
            else:
                return True
        else:
            return False

    def get_trace(self):
        """
        生成运行轨迹
        """
        lu = [0.7, 0.3]
        # 创建存放轨迹信息的列表
        trace = []

        faster_distance = 260
        for i in lu:
            the_distance = i * faster_distance
            # 设置初始位置、初始速度、时间间隔
            start, v0, t = 0, 0, 1
            # 当尚未移动到终点时
            while start < the_distance:
                # 如果处于加速阶段
                if start < the_distance:
                    # 设置加速度为2
                    a = 2
                # 如果处于减速阶段
                else:
                    # 设置加速度为-3
                    a = -3
                # 移动的距离公式
                move = v0 * t + 1 / 2 * a * t * t
                # 此刻速度
                v = v0 + a * t
                # 重置初速度
                v0 = v
                # 重置起点
                start += move
                # 将移动的距离加入轨迹列表
                trace.append(round(move, 2))
        # 返回轨迹信息
        return trace

    def show_chrome_top(self, hwnd):
        """
        将窗口至于前方
        """
        win32gui.SetForegroundWindow(hwnd)

    def save_chrome_image(self, hwnd, img_name='output.png'):
        """
        保存窗口图片
        """
        # 获取窗口的坐标位置
        left, top, right, bot = win32gui.GetWindowRect(hwnd)
        width = right - left
        height = bot - top
        # 保存图片相关
        hWndDC = win32gui.GetWindowDC(hwnd)
        mfcDC = win32ui.CreateDCFromHandle(hWndDC)
        saveDC = mfcDC.CreateCompatibleDC()
        saveBitMap = win32ui.CreateBitmap()
        saveBitMap.CreateCompatibleBitmap(mfcDC, width, height)
        saveDC.SelectObject(saveBitMap)
        saveDC.BitBlt((0, 0), (width, height), mfcDC, (0, 0), win32con.SRCCOPY)
        saveBitMap.SaveBitmapFile(saveDC, img_name)

    def get_box_coordinate(self, imgsrc, imgobj, confidencevalue=0.5):
        """
        imgsrc: 原图
        imgobj： 要查找的图片（部分图片）
        """
        imsrc = ac.imread(imgsrc)
        imobj = ac.imread(imgobj)
        match_result = ac.find_template(imsrc, imobj, confidencevalue)
        if match_result is None:
            return None
        else:
            return match_result.get('result')

    def save_csv(self, result):
        csv_title = ["province", "city", "url","source"]
        file_name = "source.csv"
        if not os.path.exists(file_name):
            with open(file_name, "w", encoding="utf8", newline="\n") as fp:
                write = csv.writer(fp, delimiter=chr(9578))
                write.writerow(csv_title)
        savedata = list(map(lambda key: result.get(key), csv_title))
        with open(file_name, "a", encoding="utf8", newline="\n") as fp:
            write = csv.writer(fp, delimiter=chr(9578))
            write.writerow(savedata)
            print(f'save:{result["url"]} success! ')

    def save_list(self,result):
        csv_title = ["province", "city", "url"]
        file_name = "listpage.csv"
        if not os.path.exists(file_name):
            with open(file_name, "w", encoding="utf8", newline="\n") as fp:
                    write = csv.writer(fp, delimiter=chr(9578))
                    write.writerow(csv_title)
        savedata = list(map(lambda key: result.get(key), csv_title))
        with open(file_name, "a", encoding="utf8", newline="\n") as fp:
            write = csv.writer(fp, delimiter=chr(9578))
            write.writerow(savedata)
            print(f'save_list:{result["url"]} success! ')

        # self.number += 1
        # if self.number >= 5000:  # 自动停止点
        #     return 0
        # else:
        #     return 1

    def save_source(self, url, html):
        """存储网页源代码"""
        save_path = os.path.join("result", "网页源代码")
        if not os.path.exists(save_path):
            os.makedirs(save_path)
        with open(os.path.join(save_path, url.rsplit("/", 1)[-1].replace(".htm", ".html")), mode='w', encoding='utf-8') as fp:
            fp.write(html.replace('charset=gbk"','charset=utf-8"'))
        print(f'save:{url.rsplit("/", 1)[-1].replace(".htm", ".html")} success! ')

    @classmethod
    def clear_chrome_logs(cls):
        if os.path.exists(Chrome_Log_Path):
            shutil.rmtree(Chrome_Log_Path)


if __name__ == '__main__':
    # 启动登录之后，程序会自动点击到房产法拍页面，此时可能会跳转到登录界面，也可能不会跳转到登录界面，此时必须去点击登录完成账号登录才行
    start_page = 1  # 只有采集列表页才有效果，采集详情页设置成1就可以了
    crawler_list = False# 用于指定采集的是列表页还是详情页False
    Spider.clear_chrome_logs()
    time.sleep(3)
    while True:
        res = asyncio.run(Spider().run_spider(crawler_list=crawler_list, start_page=start_page))
        # if crawler_list is False:
        #     if res is False:
        #         break
        #     elif res is True:
        #         print("下载完成!")
        #         break
        # elif isinstance(res, int) and res is not True:
        if isinstance(res, int) and res is not True:
            start_page = res
            time.sleep(30)
        else:
            print("采集列表页完成!" if crawler_list else "采集详情页完成")
            break

