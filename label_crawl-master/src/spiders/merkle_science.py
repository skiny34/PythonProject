import random

from spiders.spider_common import SpiderCommon


class MerkleScienceSpider(SpiderCommon):

    def __init__(self, config, logger, loop):
        super().__init__(config, logger, loop)

        self.merkle_science_config = self.spider_config.get("merkle_science", {})


        self.address_tag_config = self.merkle_science_config.get("address_tag", {})
        self.headers = {

            # "Content-Type": "application/json"
        }
        self.access_token = None
        self.api_key = self.address_tag_config.get("api_key")

    async def get_token_oauth(self):

        oauth_params = self.address_tag_config.get("oauth_params", {})
        oauth_url =  self.address_tag_config.get("oauth_url")

        self.headers.update({
            "Content-Type":"application/x-www-form-urlencoded",
            "Auth0-Client":"eyJuYW1lIjoiYXV0aDAtcmVhY3QiLCJ2ZXJzaW9uIjoiMi4zLjAifQ==",
            "referer":"",
            'User-Agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36"

        })
        status_code, res = await self.fetch_with_retry(
            url=oauth_url,
            method="post",
            headers=self.headers,
            json=oauth_params,
            # is_json=True
        )

        if status_code != 200:
            self.logger.error("获取OAuth令牌失败，状态码: {}".format(status_code))
            return None

        if not res.get("access_token"):
            self.logger.error("获取OAuth令牌失败，响应内容: {}".format(res))
            return None

        return res.get("access_token")



    async def address_tag_query(self, chain: str, address: str):
        """

        :param chain:
        :param address:
        :return:
        """

        url =  self.address_tag_config.get("url")

        # 请求接口

        json = {"input": address}
        self.headers.update({
            "Authorization":f"Bearer {self.api_key}"
        })
        # oauth_info = await self.get_token_oauth()
        status_code, res = await self.fetch_with_retry(
            url=url,
            method="post",
            json=json,
            headers=self.headers,
            is_json=True
        )

        if status_code != 200:
            self.logger.error(f"请求失败，状态码: {status_code}")
            return None

        address_tags = res.get("addresses")
        if not address_tags:
            return None
        chain_conversion = self.address_tag_config.get("chain_conversion", {})
        chain_tags_addr = address_tags.get(chain_conversion.get(chain))
        if not chain_tags_addr:
            self.logger.info(f"平台 merkle science 地址 {address} 在链 {chain} 上没有标签信息")
            return None

        tags_name  = chain_tags_addr[0].get("tag_name_verbose")
        tag_type = chain_tags_addr[0].get("tag_type_verbose")

        tags = f"{tags_name}-{tag_type}"
        self.logger.info("成功抓取 地址{} merkle science 标签 --{}--".format(address, tags))
        return tags
