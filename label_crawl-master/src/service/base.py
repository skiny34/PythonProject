import asyncio
import json
import openpyxl
from nb_conn.es.async_helper import AsyncEs
from nb_conn.mongodb.util import UpdateOneHelper

from helper.interface import Interface
from spiders.arkham import ArkhamSpider
from spiders.baidu_images import BaiduImageSpider
from spiders.btc_wallet_explorer import BtcWalletExport
from spiders.chainabuse import ChainAbuseSpider
from spiders.merkle_science import MerkleScienceSpider
from spiders.mist_track import MistTrackSpider
from spiders.oklink import OklinkSpider
from spiders.scan import ScanSpider
from storage.mongo import MongodbStorage
from storage.obs import TGOBSStorage


class Base:

    def __init__(self, config, logger, loop):
        self.config = config
        self.loop = loop
        self.logger = logger
        self.oklink_spider = None  # Placeholder for OklinkSpider instance
        self.arkham_spider = None  # Placeholder for OklinkSpider instance
        self.scan_spider = None  # Placeholder for OklinkSpider instance
        self.chain_abuse_spider = None  # Placeholder for ChainAbuseSpider instance
        self.merkle_science_spider = None  # Placeholder for MerkleScienceSpider instance
        self.mist_track_spider = None  # Placeholder for MerkleScienceSpider instance
        self.baidu_image_spider = None
        self.wallet_explorer_spider = None
        self.obs = None
        self._init_obs()
        self._init_spider()
        self.es = None
        self.evm_chains = config.get("EVM_CHAINS", [])
        self.es_config = config.get("ES",{})
        self.mongo_config = config.get("MONGODB",{})
        self.interface = Interface()

        self.headers = {
            "Content-Type": "application/json",
            "token": "423f953e0039ee84eb73e47173b99691"
        }
        self._init_mongo()
        self._init_es()


    def _init_obs(self):
        """

        :return:
        """
        obs_config = self.config.get("OBS", {})
        if not obs_config:
            raise ValueError("OBS configuration is missing in the config file.")

        self.obs = TGOBSStorage(obs_config.get("conn",{}))

    def _init_es(self):
        """
        :return:
        """
        conn = self.es_config.get("client")
        self.es = AsyncEs(**conn)

    def _init_mongo(self):

        self.mongo_helper = MongodbStorage(self.mongo_config)
        self.tag_mongo = self.mongo_helper.tag_mongo
        self.data_mongo = self.mongo_helper.data_mongo
        self.tag_algorithm_mongo = self.mongo_helper.tag_algorithm_mongo


    def _init_spider(self):
        self.oklink_spider = OklinkSpider(config=self.config, loop=self.loop, logger=self.logger)
        self.arkham_spider = ArkhamSpider(config=self.config, loop=self.loop, logger=self.logger)
        self.scan_spider = ScanSpider(config=self.config, loop=self.loop, logger=self.logger)
        self.chain_abuse_spider = ChainAbuseSpider(config=self.config, loop=self.loop, logger=self.logger)
        self.merkle_science_spider = MerkleScienceSpider(config=self.config, loop=self.loop, logger=self.logger)
        self.mist_track_spider = MistTrackSpider(config=self.config, loop=self.loop, logger=self.logger)
        self.baidu_image_spider = BaiduImageSpider(config=self.config, loop=self.loop, logger=self.logger)
        self.wallet_explorer_spider = BtcWalletExport(config=self.config, loop=self.loop, logger=self.logger)

    async def save_mongo_data(self, data, coll,db):
        """
        疑似热钱包存储
        :param data:
        :param coll:
        :param db:

        :return:
        """


        bulk_updates = []
        for _info in data:
            _id = _info.pop("address")
            bulk_updates.append(
                UpdateOneHelper({"_id": _id}, {"$setOnInsert": _info}, upsert=True)
            )

        step = 500
        for i in range(0, len(bulk_updates), step):
            docs = bulk_updates[i: i + step]
            await self.tag_algorithm_mongo.collection(coll, db).bulk_write(docs)


    async def filter_tags_addresses(self, coll, addresses):

        """
        过滤已经存在的标签
        :param coll:
        :param addresses:

        :return:
        """
        result = []
        # coll = "tron_tag_rel"

        for i in range(0, len(addresses), 200):
            address_list = addresses[i:i + 200]
            async for doc in self.tag_mongo.collection(coll, "tag_database").find(
                    {
                        "address": {"$in": address_list},
                        "valid": True
                    }, ["_id", "address"]
            ):
                address = doc.get("address")
                result.append(address)

        return list(set(addresses) - set(result))

    async def _deal_tags(self, url, data, sp):
        """
        并发处理
        :param path:
        :param data:
        :param sp:
        :return:
        """
        try:
            async with sp:
                status, result = await self.interface.request(
                    url=url,
                    method="post",
                    data=json.dumps(data),
                    headers=self.headers
                )
                return status, result
        except Exception as e:
            raise Exception(f"处理标签失败 {url} {data} {e}")

    async def deal_tag_fetch_with_retry(self, url, data, sp, max_retries=3, backoff_factor=5):
        """
        实现带重试机制的请求逻辑。
        :param path: 请求的路径
        :param data: 请求的数据
        :param max_retries: 最大重试次数
        :param backoff_factor: 退避因子（指数退避基数）
        """
        for attempt in range(max_retries):
            try:
                return await self._deal_tags(url, data, sp)
            except Exception as e:
                self.logger.error(f"Path {url},处理标签失败等待重试!")
                if attempt == max_retries - 1:
                    raise  # 最后一次重试后仍失败，抛出异常
                wait_time = backoff_factor * (3 ** attempt)  # 指数退避
                self.logger.error(f"Attempt {attempt + 1} failed. Retrying in {wait_time}s...")
                await asyncio.sleep(wait_time)
        raise RuntimeError("Unreachable code")

    @staticmethod
    def export_to_excel(data, filename):
        """
        将数据导出到 Excel 文件
        :param data: 要导出的数据，应该是一个包含字典的列表
        :param filename: 输出的Excel文件名
        """
        if not data:
            print("数据为空！")
            return

        # 创建一个新的工作簿
        wb = openpyxl.Workbook()

        # 选择活动工作表
        sheet = wb.active
        sheet.title = "Sheet1"  # 设置工作表名称

        # 获取字典的所有键作为Excel的列名
        fieldnames = data[0].keys()

        # 写入列名（表头）
        sheet.append(list(fieldnames))

        # 写入数据行
        for row in data:
            sheet.append(list(row.values()))
        # 保存到文件
        wb.save(filename)
        print(f"数据成功导出到 {filename}")



