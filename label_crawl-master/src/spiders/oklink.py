import json

import requests

from spiders.spider_common import SpiderCommon

from pyquery import PyQuery as pq
class OklinkSpider(SpiderCommon):

    def __init__(self, config,logger,loop):
        super().__init__(config, logger,loop)

        self.ok_config = self.spider_config.get("oklink", {})

    async def address_tag_query(self,chain: str = '', address: str = ''):
        """

        :param chain:
        :param address:
        :return:
        """
        if not chain or not address:
            self.logger.error("链名或地址不能为空")
            return None
        url = self.ok_config.get("address_tag",{}).get(chain).format(address=address)

        res_status,res = await self.fetch_with_retry(url=url.format(address=address))
        # res1 = requests.get(url, headers=self.headers)
        if res_status in [404,403]:
            return None
        try:
            content = pq(res)('script[id="appState"][data-id="__app_data_for_ssr__"]').text()
            oklink_tag_dict = json.loads(content)['appContext']['initialProps']['store']['pageState']['tagStore'][
                'tagMaps']
            # if oklink_tag_dict.get('entityTags'):
            temp_oklink_tag = oklink_tag_dict.get('hoverEntityTag')
            if temp_oklink_tag:
                self.logger.info(f'成功抓取 地址{address} oklink 标签 --{temp_oklink_tag}--')
                return temp_oklink_tag
        except Exception as _:
            return None