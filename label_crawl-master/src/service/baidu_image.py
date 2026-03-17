import asyncio
import os

import shutil
import uuid
from collections import OrderedDict, namedtuple

from urllib.parse import urlparse

import requests
from nb_conn.mongodb.util import UpdateOneHelper


from service.base import Base
from tools.tools import fuzzy_match_addresses

Info = namedtuple('Info', 'tag value')


class BaiduImage(Base):

    def __init__(self, config, logger, loop):
        super().__init__(config, logger, loop)

        self.wallet_type = "TokenPocket"
        self.screenshot_related_a_id = "685e367cbb45e11ffe879b78"



    async def handler_images_orc(self,file_names):
        """
        处理OCR 识别
        :return:
        """
        res_data = await self.baidu_image_spider.images_ocr(file_names=file_names, directory='./images/')

        # 解析 OCR 中地址数据
        self.logger.info("开始解析 OCR 识别结果...")
        addr_data_map = await self.format_ocr_str(res_data)
        if not addr_data_map:
            self.logger.error("No valid addresses found in OCR results.")
            return None
        # 确定这批地址归属于哪些链

        return addr_data_map


    async def filter_tags_addr(self,data):
        """

        :param data:
        :return:
        """
        self.logger.info("开始过滤标签地址...")
        filter_addresses = set()
        for item in data:
            source_chain = item.get("chains")
            address = item.get("address")
            source_chain.append("evm")
            for chain in source_chain:
                coll = f"{chain}_tag_rel"
                async for doc in self.tag_mongo.collection(coll, "tag_database").find(
                        {
                            "address": address,
                            "valid": True
                        }, ["_id", "address"]
                ):
                    exits_address = doc.get("address")
                    filter_addresses.add(exits_address)


        return filter_addresses


    async def handler(self,):
        """
        处理
        :return:
        """

        res = await self.baidu_image_spider.get_baidu_url()
        # # # # 下载图片
        self.logger.info("开始下载图片到本地...")
        urls = [images_url for images_url, source_url in res.items()]

        await self.baidu_image_spider.download_images(urls)
        file_names = {images_url.split('/')[-1]: source_url for images_url, source_url in res.items()}

        source_url_map = {source_url: images_url for images_url, source_url in res.items()}

        orc_addr_data = await self.handler_images_orc(file_names)
        if not orc_addr_data:
            return None

        es_data = await self.fuzzy_addr_by_es(orc_addr_data, source_url_map)

        self.logger.info(f"ES 模糊搜索地址数量 {len(es_data)}")
        return es_data


    async def _save_to_obs(self,addr,images_path,sp):
        """

        :param images_path:
        :return:
        """
        inputs = requests.get(images_path)
        content = inputs.content
        file_name = f"{self.wallet_type}/{addr}-{str(uuid.uuid4())}.jpg"
        async with sp:

            res = await self.obs.save_image(file_name,content)

        obj_url = res.body.objectUrl

        return {addr:obj_url}

    async def images_save_to_obs(self,images_data_map):

        """
        把图片链接 存储到OBS
        :param images_data:
        :return:
        """
        exports_addr = []
        addr_obs_map = {}
        try:

            max_value = min(5,len(images_data_map))
            semaphore = asyncio.Semaphore(max_value)

            tasks = []
            for images_url,addresses in images_data_map.items():
                if len(addresses) > 2:
                    self.logger.warning(f"Skipping large batch of addresses for {images_url}: {addresses}")
                    for addr in addresses:
                        exports_addr.append({
                            "address":addr,
                            "images_url":images_url
                        })
                    continue
                for addr in addresses:

                    tasks.append(self._save_to_obs(addr,images_url,semaphore))

            res = await asyncio.gather(*tasks)

            for item in res:
                for addr,obs_url in item.items():
                    addr_obs_map[addr] = obs_url

            return addr_obs_map,exports_addr
        except Exception as e:
            self.logger.error(f"Error saving images to OBS: {e}")

    async def run(self):
        """
        查询搜索百度相似图片，并识别OCR 存入数据
        :return:
        """
        try:

            self.logger.info("开始处理 Baidu image service...")
            results_data = await self.handler()

            # 存在标签的地址
            if not results_data:
                return
            tags_addresses = await self.filter_tags_addr(results_data)

            if not results_data:
                self.logger.error("No results data found, exiting Baidu image service.")
                return
            # 因为存在一个图片涉及多个地址,避免同一张图片多次存储
            addr_images_map = {}
            for item in results_data:
                images_url = item.get("images_url")
                addr = item.get("address")
                if addr in tags_addresses:
                    continue
                _images = addr_images_map.setdefault(images_url,[])
                _images.append(addr)
            # 存储到OBS
            if not addr_images_map:
                self.logger.error("No valid address images found to save to OBS.")
                return
            obs_key_value,exports_addr = await self.images_save_to_obs(addr_images_map)

            self.export_to_excel(exports_addr,"more_addr_images.xlsx")
            self.logger.info("开始存储标签数据数据到数据库...")
            await self.save_images_data_tags(results_data,obs_key_value)

        except Exception as e:
            raise e
        finally:
            # 删除录目
            self.logger.info("Cleaning up local images directory...")
            await self.delete_images_directory('./images')

            self.logger.info("Baidu image service run completed.")


    async def delete_images_directory(self,directory_path):
        """

        :param directory_path:
        :return:
        """
        """
        删除本地图片目录
        :param directory_path:
        :return:
        """
        try:
            if os.path.exists(directory_path):
                shutil.rmtree(directory_path)

            else:
                self.logger.info(f"目录不存在: {directory_path}")
        except Exception as e:
            self.logger.error(f"Error deleting local images directory: {e}")

    async def format_ocr_str(self, res_data):
        """
        处理 ocr 识别的图片数据，提取地址
        :param res_data:
        :return:
        """
        results = {}
        for item in res_data:
            if not item:
                continue
            images_url = item.get("file_path")
            rec_texts = item.get("rec_texts")
            rec_texts = "".join(rec_texts)
            res = fuzzy_match_addresses(rec_texts)
            if res:
                for r in res:
                    direction = r.get("direction")
                    fuzzy_addr = r.get("fuzzy_addr")
                    if fuzzy_addr.startswith('Ox'):
                        # EVM 地址
                        fuzzy_addr = fuzzy_addr.replace('O', '0')
                    rs = fuzzy_addr.replace('.', '*')
                    results[rs] = {"direction": direction, "file_path": images_url}

        return results

    async def _fuzzy_addr(self, text_addr, file_path, direction, sp):
        """
        模糊地址匹配
        :param text_addr:
        :param sp:
        :return:
        """
        rsp_chain_addr_map = {}
        async with sp:
            # 查询所有地址表
            addr_index = "*_address_balance"
            body = {
                "query": {
                    "wildcard": {
                        "address": text_addr

                    }
                }
            }

            data = await self.es.search_index_docs(index=addr_index, body=body)
            hits = data.get("hits", {}).get("hits", [])
            chains = []
            for doc in hits:
                _index = doc.get("_index")
                chain = _index.split("_address_balance")[0]
                address = doc.get("_source", {}).get("address")
                chains.append(chain)
                _chain_addr = rsp_chain_addr_map.setdefault(address, {})
                _chain_addr.update({
                    "chains": chains,
                    "address": address,
                    "file_url": file_path,
                    "direction": direction
                })

        return rsp_chain_addr_map

    async def fuzzy_addr_by_es(self, text_addr_data, source_url_map):
        """
        ES 匹配链地址
        :param text_addr_data:
        :param source_url_map:
        :return:
        """
        results = []
        max_value = min(5, len(text_addr_data))

        semaphore = asyncio.Semaphore(max_value)

        tasks = []
        for text_addr, data in text_addr_data.items():
            file_path = data.get("file_path")
            direction = data.get("direction")

            tasks.append(self._fuzzy_addr(text_addr, file_path, direction, semaphore))

        res = await asyncio.gather(*tasks)

        for r in res:
            for addr, items in r.items():
                source_url = items.get("file_url")
                # 提取来源链接域名
                parsed = urlparse(source_url)
                domain = parsed.netloc
                domain_str = domain.split(':')[0]
                results.append({
                    "address": addr,
                    "chains": items.get("chains", []),
                    "source_url": items.get("file_url"),
                    "direction": items.get("direction"),
                    "wallet_type": self.wallet_type,
                    "images_url": source_url_map.get(items.get("file_url"), ""),
                    "domain": domain_str,

                })
                # 存储到MongoDB

        return results

    async def filter_tags_addresses(self, colls, addresses):

        """
        过滤已经存在的标签
        :param coll:
        :param addresses:

        :return:
        """
        result = []
        for coll in colls:

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

    async def save_images_data_tags(self,data,obs_key_value):
        """

        :param data:
        :param obs_key_value:
        :return:
        """

        max_value = min(3, len(data))
        semaphore = asyncio.Semaphore(max_value)
        save_path = "http://124.71.207.25:9001/api/v1/tag/add_tag_rel"
        tasks = []

        for item in data:
            addr = item.get("address")
            source_chain = item.get("chains")
            source_chain = list(set(source_chain))
            d = item.get("direction")
            wallet_type = item.get("wallet_type")
            obs_images_url = obs_key_value.get(addr) if obs_key_value.get(addr) else ""
            source_url = item.get("source_url")
            data_list = [str(source_chain),d,wallet_type]

            remark = ",".join(data_list)
            domain = item.get("domain")
            source_chain = source_chain[0]
            if source_chain in self.evm_chains:
                chain = "evm"
            else:
                chain = source_chain
            insert_data = {
                "a_id": self.screenshot_related_a_id,
                "name_zh": f"链下关联信息涉及【{domain}】",
                "name_en": domain,
                "description_zh": "",
                "description_en": "",
                "source_chain": source_chain,
                "chain": chain,
                "address": addr,
                "source": 1,
                "link": source_url,
                "link2":obs_images_url,
                "occurred_time": 0,
                "remark": remark,
                "source_type":80
            }

            tasks.append(asyncio.create_task(self.deal_tag_fetch_with_retry(save_path, insert_data, semaphore)))

        await asyncio.gather(*tasks)


    async def save_images_str_to_mongo(self, data):
        """
        存储图片识别结果到MongoDB
        :param data:
        :return:
        """

        if not data:
            self.logger.error("No data to save to MongoDB.")
            return

        try:
            bulk_updates = []
            for item in data:
                _id = item.pop("address")
                bulk_updates.append(
                    UpdateOneHelper({
                        "_id": _id
                    }, {"$set": item}, upsert=True)
                )
            step = 500
            for i in range(0, len(bulk_updates), step):
                docs = bulk_updates[i: i + step]
                await self.tag_algorithm_mongo.collection("images_ocr", "tag_database").bulk_write(docs)

            self.logger.info("Data saved to MongoDB successfully.")
        except Exception as e:
            self.logger.error(f"Failed to save data to MongoDB: {e}")
