import asyncio
import os
import random
import typing

import pandas as pd
import requests


from helper.interface import Interface


class SpiderCommon:

    def __init__(self, config, logger, loop):
        self.config = config
        self.loop = loop
        self.logger = logger

        self.spider_config = self.config.get("SPIDERS", {})
        self.interface = Interface()
        self.user_agent_list = self.config.get("USER_AGENT_LIST", [])
        self.headers = {
            'User-Agent': random.choice(self.user_agent_list),
            # 'Content-Type':"application/json"
        }


    @staticmethod
    def write_csv(data_list: typing.List[dict] = None, file_path: str = '') -> None:
        df = pd.DataFrame(data_list)
        if os.path.exists(file_path):
            df.to_csv(file_path, mode='a', header=False, index=False, encoding='utf-8')
        else:
            df.to_csv(file_path, index=False, encoding='utf-8')

    async def retry_requests(self,
                             url,
                             method="get",
                             params=None,
                             json=None,
                             # headers=None,
                             max_retries=3,
                             backoff_factor=5,
                             **kwargs):
        for attempt in range(max_retries):
            try:
                if method == "get":
                    return requests.get(url,params,  **kwargs)
                else:
                    return requests.post(url,json,**kwargs)
            except Exception as e:
                self.logger.error(f"Url {url},请求失败，准备重试!")
                if attempt == max_retries - 1:
                    raise  # 最后一次重试后仍失败，抛出异常
                wait_time = backoff_factor * (3 ** attempt)  # 指数退避
                self.logger.error(f"Attempt {attempt + 1} failed. Retrying in {wait_time}s...")
                await asyncio.sleep(wait_time)
        raise RuntimeError("Unreachable code")





    async def fetch_with_retry(self,
                               url,
                               method="get",
                               headers=None,
                               is_json=False,
                               max_retries=3,
                               backoff_factor=5,
                               **kwargs
                               ):
        """
        实现带重试机制的请求逻辑。
        :param url: 请求url
        :param method: 请求方法（默认为GET）
        :param headers: 请求头（默认为None）
        :param is_json: 是否返回JSON格式（默认为False）
        :param max_retries: 最大重试次数
        :param backoff_factor: 退避因子（指数退避基数）
        """
        for attempt in range(max_retries):
            try:
                return await self.process_request(url, method, headers, is_json, **kwargs)
            except Exception as e:
                self.logger.error(f"Url {url},请求失败，准备重试!")
                if attempt == max_retries - 1:
                    raise  # 最后一次重试后仍失败，抛出异常
                wait_time = backoff_factor * (3 ** attempt)  # 指数退避
                self.logger.error(f"Attempt {attempt + 1} failed. Retrying in {wait_time}s...")
                await asyncio.sleep(wait_time)
        raise RuntimeError("Unreachable code")

    async def address_tag_query(self, addr: str, chain: str):
        """
        查询地址标签
        :param addr: 地址
        :param chain: 链名
        :return: 标签字典或None
        """
        raise NotImplementedError("This method should be implemented by subclasses")

    async def process_request(self,
                              url: str,
                              method: str = 'get',
                              headers: typing.Optional[typing.Dict] = None,
                              is_json=False,
                              **kwargs
                              ):

        try:

            res_status, res = await self.interface.request(
                method=method,
                url=url,
                headers=headers if headers else self.headers,
                is_json=is_json,
                **kwargs
            )
            if res_status in [404, 403]:
                self.logger.error(f"Request to {url} returned  Not Found")
                return res_status, None
            elif res_status == 200:
                return res_status, res
            else:
                raise Exception(f"Unexpected status code {res_status} for {url}")
        except Exception as e:
            raise Exception(f"Request failed for {url}: {e}")

