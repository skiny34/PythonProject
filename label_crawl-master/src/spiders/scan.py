from spiders.spider_common import SpiderCommon
from pyquery import PyQuery as pq

class ScanSpider(SpiderCommon):

    def __init__(self, config,logger,loop):
        super().__init__(config,logger,loop)

        self.scan_config = self.spider_config.get("scan", {})

        self.address_tag_config = self.scan_config.get("address_tag", {})





    async def address_tag_query(self, addr: str, chain: str):
        """

        :param addr:
        :param chain:
        :return:
        """
        url = self.address_tag_config.get(chain).format(address=addr)


        try:
            res_status, res = await self.fetch_with_retry(url=url, method="get")

            if res_status in [404, 403]:
                return None
            tag = pq(res)('title').text()
            if tag:
                tag = tag.split('|')[0].strip()
                if 'address' in tag.lower():
                    tag = None
            if not tag:
                return None

            self.logger.info(f'成功抓取 地址{addr} scan 标签 --{tag}--')
            return tag
        except Exception as e:
            raise e

