import csv
import json
import os
import random
import re
import sys
import warnings
from collections import OrderedDict
from time import sleep
from lxml.etree import HTML

import requests

warnings.filterwarnings("ignore")


def insert_or_update_user(headers, result_data, file_path):
    """插入或更新用户csv。不存在则插入。"""
    first_write = True if not os.path.isfile(file_path) else False

    # 没有或者新建
    result_data[0].append('')
    with open(file_path, 'a', encoding='utf-8-sig', newline='') as f:
        writer = csv.writer(f)
        if first_write:
            writer.writerows([headers])
        writer.writerows(result_data)
    print('{} 信息写入csv文件完毕，保存路径: {}'.format(result_data[0][1], file_path))


class Weibo(object):
    def __init__(self, config):
        """Weibo类初始化"""
        self.user_csv_file_path = None
        self.result_dir_name = 0
        cookie = config.get("cookie")  # 微博cookie，可填可不填
        user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36"
        self.headers = {"User_Agent": user_agent, "Cookie": cookie}
        user_id_list = config["user_id_list"]

        self.uid_file_path = ""
        uid_list = [
            user_id
            for user_id in user_id_list
        ]
        self.uid_list = uid_list  # 要爬取的微博用户的uid列表
        self.user_num = config.get("user_num", len(self.uid_list))
        self.uid = ""  # 用户id
        self.user = {}  # 存储目标微博用户信息

    def get_json(self, params):
        """获取网页中json数据"""
        url = "https://m.weibo.cn/api/container/getIndex?"
        r = requests.get(url, params=params, headers=self.headers, verify=False)
        return r.json(), r.status_code

    def user_to_csv(self):
        """将爬取到的用户信息写入csv文件"""
        file_dir = os.path.split(os.path.realpath(__file__))[0] + os.sep + "weibo"
        if not os.path.isdir(file_dir):
            os.makedirs(file_dir)
        file_path = file_dir + os.sep + "users.csv"
        self.user_csv_file_path = file_path
        result_headers = [
            "用户id",
            "昵称",
            "性别",
            "生日",
            "所在地",
            "学习经历",
            "公司",
            "注册时间",
            "阳光信用",
            "微博数",
            "粉丝数",
            "关注数",
            "简介",
            "主页",
            "头像",
            "高清头像",
            "微博等级",
            "会员等级",
            "是否认证",
            "认证类型",
            "认证信息",
        ]
        result_data = [
            [
                v.encode("utf-8") if "unicode" in str(type(v)) else v
                for v in self.user.values()
            ]
        ]
        # 已经插入信息的用户无需重复插入，返回的id是空字符串或微博id 发布日期%Y-%m-%d
        insert_or_update_user(result_headers, result_data, file_path)

    def get_user_info(self):
        """获取用户信息"""
        params = {"containerid": "100505" + str(self.uid)}
        # 休眠时长
        sleep(random.randint(3, 10))
        js, status_code = self.get_json(params)
        if status_code != 200:
            print("被ban了，需要等待一段时间")
            sys.exit()
        if js["ok"]:
            info = js["data"]["userInfo"]
            user_info = OrderedDict()
            user_info["id"] = self.uid
            user_info["screen_name"] = info.get("screen_name", "")
            user_info["gender"] = info.get("gender", "")
            params = {
                "containerid": "230283" + str(self.uid) + "_-_INFO"
            }
            zh_list = ["生日", "所在地", "小学", "初中", "高中", "大学", "公司", "注册时间", "阳光信用"]
            en_list = [
                "birthday",
                "location",
                "education",
                "education",
                "education",
                "education",
                "company",
                "registration_time",
                "sunshine",
            ]
            for i in en_list:
                user_info[i] = ""
            js, _ = self.get_json(params)
            if js["ok"]:
                cards = js["data"]["cards"]
                if isinstance(cards, list) and len(cards) > 1:
                    card_list = cards[0]["card_group"] + cards[1]["card_group"]
                    for card in card_list:
                        if card.get("item_name") in zh_list:
                            user_info[
                                en_list[zh_list.index(card.get("item_name"))]
                            ] = card.get("item_content", "")
            user_info["statuses_count"] = self.string_to_int(
                info.get("statuses_count", 0)
            )
            user_info["followers_count"] = self.string_to_int(
                info.get("followers_count", 0)
            )
            user_info["follow_count"] = self.string_to_int(info.get("follow_count", 0))
            user_info["description"] = info.get("description", "")
            user_info["profile_url"] = info.get("profile_url", "")
            user_info["profile_image_url"] = info.get("profile_image_url", "")
            user_info["avatar_hd"] = info.get("avatar_hd", "")
            user_info["urank"] = info.get("urank", 0)
            user_info["mbrank"] = info.get("mbrank", 0)
            user_info["verified"] = info.get("verified", False)
            user_info["verified_type"] = info.get("verified_type", -1)
            user_info["verified_reason"] = info.get("verified_reason", "")
            user = self.standardize_info(user_info)
            self.user = user
            self.user_to_csv()
            return 0
        else:
            print("user_id_list中 {} id出错".format(self.uid))
            return -1

    def string_to_int(self, string):
        """字符串转换为整数"""
        if isinstance(string, int):
            return string
        elif string.endswith("万+"):
            string = string[:-2] + "0000"
        elif string.endswith("万"):
            string = float(string[:-1]) * 10000
        elif string.endswith("亿"):
            string = float(string[:-1]) * 100000000
        return int(string)

    def standardize_info(self, weibo):
        """标准化信息，去除乱码"""
        for k, v in weibo.items():
            if (
                    "bool" not in str(type(v))
                    and "int" not in str(type(v))
                    and "list" not in str(type(v))
                    and "long" not in str(type(v))
            ):
                weibo[k] = (
                    v.replace("\u200b", "")
                    .encode(sys.stdout.encoding, "ignore")
                    .decode(sys.stdout.encoding)
                )
        return weibo

    def initialize_info(self, uid):
        """初始化爬虫信息"""
        self.user = {}
        self.uid = uid

    def get_following(self):
        url = "https://weibo.cn/%s/fans?page=1" % self.uid
        response = requests.get(url, headers=self.headers, verify=False)
        selector = HTML(response.text.encode("utf-8"))
        urls = selector.xpath('//a[text()="关注他" or text()="关注她" or text()="移除"]/@href')
        uids = re.findall('uid=(\d+)', ";".join(urls), re.S)
        for uid in uids:
            if uid not in self.uid_list:
                self.uid_list.append(uid)

    def start(self):
        """运行爬虫"""
        try:
            num = 0
            for uid in self.uid_list:
                self.initialize_info(uid)
                if len(self.uid_list) < self.user_num:
                    self.get_following()
                # else:
                #     with open("uids.json", 'w',encoding='utf-8') as fd:
                #         json.dump({'uids': self.uid_list[0:self.user_num]}, fd)
                #     break
                if self.get_user_info() != 0:
                    return
                num += 1
                if num == self.user_num:
                    break
                print("已抓取 %d/%d" % (num, self.user_num))
                print("*" * 100)
        except Exception as e:
            print(e)


def get_config():
    """获取config.json文件信息"""
    config_path = os.path.split(os.path.realpath(__file__))[0] + os.sep + "config.json"
    if not os.path.isfile(config_path):
        print(
            "当前路径：%s 不存在配置文件config.json",
            (os.path.split(os.path.realpath(__file__))[0] + os.sep),
        )
        sys.exit()
    try:
        with open(config_path, encoding="utf-8") as f:
            config = json.loads(f.read())
            return config
    except ValueError:
        print(
            "config.json 格式不正确")
        sys.exit()


def main():
    try:
        config = get_config()
        wb = Weibo(config)
        wb.start()  # 爬取微博信息
    except Exception as e:
        print(e)


if __name__ == "__main__":
    main()
