from mailbox import NotEmptyError



from spiders.spider_common import SpiderCommon


from pyquery import PyQuery as pq

class BtcWalletExport(SpiderCommon):

    def __init__(self, config, logger, loop):
        super().__init__(config, logger, loop)

        self.wallet_explorer_config = self.spider_config.get("btc_wallet_explorer", {})

        self.entity_url = self.wallet_explorer_config.get("entity_url")
        self.wallet_api_url = self.wallet_explorer_config.get("wallet_api_url")




    async def get_wallet_by_api(self,entity_name):
        """

        :param entity_name:
        :return:
        """



        results = {}
        entity_name = entity_name.replace(" ","-")

        params = {
            "wallet": entity_name,
            "from": 0,
            "count":100,

        }
        url = self.wallet_api_url.format(entity_name=entity_name)


        res_status, res = await self.fetch_with_retry(url=url, params=params,is_json=True,headers=self.headers)


        if res_status in [404, 403]:
            return None
        offset = 1000
        count = res.get("addresses_count",0)
        for i in range(1,count//1000):
            self.logger.info(f"查询实体{entity_name},总共次数{count//1000},当前次数{i}")
            params.update({
                "from": (i-1)*offset,
                "count":offset,
            })
            res_status, res_temp = await self.fetch_with_retry(url=url, params=params,is_json=True,headers=self.headers)
            if res_status in [404, 403]:
                return None


            for r in res_temp.get("addresses",[]):
                r["entity_name"] = entity_name
                address = r.get("address")
                _r = results.setdefault(address,{})
                _r.update(r)

        return results

    async def get_entity(self):
        """

        :param address:
        :return:
        """
        entity_names = []
        res_status, res = await self.fetch_with_retry(url=self.entity_url)
        # res1 = requests.get(url, headers=self.headers)
        if res_status in [404, 403]:
            return None

        selects = pq(res)('div.body-wrapper1 > div.body-wrapper2 > table > tr > td ').items()

        if not selects:
            raise ValueError("未找到相关标签信息")

        for select in selects:
            ul = select.find('ul > li').items()
            for ul_s in ul:
                entity_name = ul_s.find("a").text()

                if not entity_name:
                    raise  NotEmptyError("为找到相关实体")
                entity_names.append(entity_name)
        return entity_names



