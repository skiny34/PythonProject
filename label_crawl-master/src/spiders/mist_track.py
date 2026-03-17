from spiders.spider_common import SpiderCommon


class MistTrackSpider(SpiderCommon):

    def __init__(self, config, logger, loop):
        super().__init__(config, logger, loop)

        self.mist_track_config = self.spider_config.get("mist_track", {})


        self.address_tag_config = self.mist_track_config.get("address_tag", {})


    async def address_tag_query(self, chain: str, address: str):
        """
        :param chain:
        :param address:
        :return:
        """

        chain_str = self.address_tag_config.get("chain_conversion").get(chain)
        url = self.address_tag_config.get("url", "").format(address=address, coin=chain_str)
        headers = {'Cookie': 'sessionid=21kobplo7i9430vjzs3lzc5u2k5rc3pt'}
        res_status, res = await self.fetch_with_retry(url=url, method="get",headers=headers,is_json=True)

        if res_status in [404, 403]:
            return None

        try:
            tags = res.get("address_label_list", [])
            if not tags:
                return None
            tags_str = "-".join(tags)
            self.logger.info(f'成功抓取 地址{address} mist track 标签 --{tags_str}--')
            return tags_str
        except Exception as e:
            raise e