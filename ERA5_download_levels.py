# !/usr/bin/env python3
# -*- coding:utf-8 -*-
# @Time         : 2020/4/10 16:00
# @Author       : Huanhuan sun
# @Email        : sun58454006@163.com
# @File         : ERA5_download_levels.py
# @Software     : PyCharm
# @Project      : intnet_crawler

from queue import Queue
from threading import Thread
import cdsapi
from time import time
import datetime
import os

# VARS = ['geopotential','u_component_of_wind','v_component_of_wind','temperature','relative_humidity']
VARS = ['u_component_of_wind', 'v_component_of_wind', 'vertical_velocity']


# for i in range(len(VARS)):
def downloadonefile(riqi):
    ts = time()
    # filename="levels/"+str(VARS[i])+"/era5."+str(VARS[i])+"."+riqi+".nc"
    filename = "levels/" + "era5." + riqi + ".nc"
    if os.path.isfile(filename):  # 如果存在文件名则返回
        print("ok", filename)
    else:
        print(filename)
        c = cdsapi.Client()
        c.retrieve(
            'reanalysis-era5-pressure-levels',
            {
                'product_type': 'reanalysis',
                'format': 'netcdf',  # Supported format: grib and netcdf. Default: grib
                'variable': VARS,
                'year': riqi[0:4],
                'month': riqi[-4:-2],
                'day': riqi[-2:],
                'pressure_level': [
                    '500', '550', '600',
                    '650', '700', '750',
                    '775', '800', '825',
                    '850', '875', '900',
                    '925', '950', '975',
                    '1000'
                ],
                'time': [
                    '00:00', '01:00', '02:00',
                    '03:00', '04:00', '05:00',
                    '06:00', '07:00', '08:00',
                    '09:00', '10:00', '11:00',
                    '12:00', '13:00', '14:00',
                    '15:00', '16:00', '17:00',
                    '18:00', '19:00', '20:00',
                    '21:00', '22:00', '23:00'
                ],
                'area': [60, 70, 9, 160],  # North, West, South, East. Default: global
                # 'grid': [1.0, 1.0], # Latitude/longitude grid: east-west (longitude) and north-south resolution (latitude). Default: 0.25 x 0.25
            },
            filename)


# 下载脚本
class DownloadWorker(Thread):
    def __init__(self, queue):
        Thread.__init__(self)
        self.queue = queue

    def run(self):
        while True:
            # 从队列中获取任务并扩展tuple
            riqi = self.queue.get()
            downloadonefile(riqi)
            self.queue.task_done()


# 主程序
def main():
    # 创建目录
    # for i in range(len(VARS)):
    #     os.chdir('/raid03/users/fuzp/PRESSURE/')
    #     print(os.getcwd())
    #     os.mkdir(str(VARS[i]))

    # dirs = os.listdir('/raid03/users/fuzp/PRESSURE/')

    # 起始时间
    ts = time()

    # 起始日期
    begin = datetime.date(2020, 1, 1)
    end = datetime.date(2020, 1, 31)
    d = begin
    delta = datetime.timedelta(days=1)

    # 建立下载日期序列
    links = []
    while d <= end:
        riqi = d.strftime("%Y%m%d")
        links.append(str(riqi))
        d += delta

    # 创建一个主进程与工作进程通信
    queue = Queue()

    # 注意，每个用户同时最多接受4个request https://cds.climate.copernicus.eu/vision
    # 创建四个工作线程
    for x in range(4):
        worker = DownloadWorker(queue)
        # 将daemon设置为True将会使主线程退出，即使所有worker都阻塞了
        worker.daemon = True
        worker.start()

    # 将任务以tuple的形式放入队列中
    for link in links:
        queue.put(link)

    # 让主线程等待队列完成所有的任务
    queue.join()
    print('Took {}'.format(time() - ts))


if __name__ == '__main__':
    main()