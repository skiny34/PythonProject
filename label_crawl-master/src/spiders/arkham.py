import hashlib
import time
from datetime import datetime
from urllib.parse import urlencode
import requests
from spiders.spider_common import SpiderCommon


class ArkhamSpider(SpiderCommon):

    def __init__(self, config, logger, loop):
        super().__init__(config, logger, loop)

        self.arkham_config = self.spider_config.get("arkham", {})

    def chain_to_arkham_str(self, chains):
        """

        :param chains:
        :return:
        """
        if not chains:
            return None
        arkham_chain_map = {
            "btc": "bitcoin",
            "eth": "ethereum",
            "solana": "solana",
            "tron": "tron",
            "doge": "dogecoin",
            "ton": "ton",
            "base": "base",
            "arbitrum": "arbitrum_one",
            "optimism": "optimism",
            "mantle": "mantle",
            "avalanche": "avalanche",
            "bsc": "bsc",
            "linea": "linea",
            "polygon": "polygon",
            "blast": "blast",
            "manta": "manta",
            "flare": "flare"
        }
        arkham_str = []

        chain_list = chains.split(',')
        for chain in chain_list:
            arkham_str.append(arkham_chain_map.get(chain.lower(), chain))
        return ','.join(arkham_str)

    @staticmethod
    def encrypt_x_payload(api: str = '') -> str:
        """加密函数"""
        ty = str(int(time.time()))
        tk = "gh67j345kl6hj5k432"
        api = api.replace('https://api.arkm.com', '')
        string_payload = f'{api.split("?")[0]}:{ty}:{tk}'
        encrypt_payload = hashlib.sha256(string_payload.encode('utf-8')).hexdigest()
        last_string_payload = f'{tk}:{encrypt_payload}'
        last_encrypt_payload = hashlib.sha256(last_string_payload.encode('utf-8')).hexdigest()

        return last_encrypt_payload

    def _init_headers(self, url):
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'X-Timestamp': str(int(time.time())),
            'X-Payload': self.encrypt_x_payload(url),
            # 'X-Payload': "d23cfd5e68b9026616bd65adc01c3a5f90f246cb41f7e4d8fca735aa96ecd176",
        }
        return headers



    async def address_tag_query(self, chain: str, address: str):
        """

        :param address:
        :param chain:
        :return:
        """
        arkham_api = self.arkham_config.get("address_tag", {}).get("url")
        url = arkham_api.format(address=address)

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'X-Timestamp': str(int(time.time())),
            'X-Payload': self.encrypt_x_payload(url),
            # 'X-Payload': "d23cfd5e68b9026616bd65adc01c3a5f90f246cb41f7e4d8fca735aa96ecd176",
        }

        rep = await self.retry_requests(url,headers=headers)
        # rep = requests.get(url, headers=headers)


        if rep.status_code in [404, 403]:
            return None
        res = rep.json()
        # arkham_dict = process_arkhma_request(api=arkhamintelligence_api.format(addr=addr))
        if res.get('arkhamEntity'):
            if res.get('arkhamEntity').get('name'):
                arkham_entity = res.get('arkhamEntity').get('name')
                if res.get('arkhamLabel'):
                    if res.get('arkhamLabel').get('name'):
                        arkham_label = res.get('arkhamLabel').get('name')
                        arkham_entity += f': {arkham_label}'
                self.logger.info(f'成功抓取 地址{address} arkham 标签 --{arkham_entity}--')
                return arkham_entity

        elif res.get('arkhamLabel'):
            if res.get('arkhamLabel').get('name'):
                arkham_label = res.get('arkhamLabel').get('name')
                self.logger.info(f'成功抓取 地址{address} arkham 标签 --{arkham_label}--')
                return arkham_label
        else:
            return None

    def get_ark_tag(self,response):
        if 'arkhamEntity' in response and 'arkhamLabel' in response:
            return response['arkhamEntity']['name'] + " " + response['arkhamLabel']['name']
        elif 'arkhamEntity' in response and 'arkhamLabel' not in response:
            return response['arkhamEntity']['name']
        elif 'arkhamEntity' not in response and 'arkhamLabel' in response:
            return response['arkhamLabel']['name']

    def find_arkham_entities(self,data):
        results = []

        def recursive_search(obj):
            if isinstance(obj, dict):
                # 如果找到包含"arkhamEntity"的键
                if "arkhamEntity" in obj or "arkhamLabel" in obj :
                    # obj.update()
                    results.append(obj)
                # 继续搜索字典的所有值
                for value in obj.values():
                    recursive_search(value)
            elif isinstance(obj, list):
                # 如果是列表,遍历每个元素
                for item in obj:
                    recursive_search(item)

        recursive_search(data)
        return results

    async def get_transfer_by_time_period(self, entity_name, start_timestamp, end_timestamp, chains_str="all"):
        """

        :param entity_name:
        :param chains_str:
        :param start_timestamp:
        :param end_timestamp:
        :return:
        """
        url = "https://api.arkm.com/transfers"
        page_size = 1000
        sort = "asc"
        params = {
            "base": entity_name,
            "flow": "all",
            "usdGte": "1",
            "sortKey": "time",
            "sortDir": sort,
            "limit": str(page_size),

        }
        if chains_str != "all":
            params["chains"] = self.chain_to_arkham_str(chains_str)
        if start_timestamp:
            params["timeGte"] = int(start_timestamp * 1000)

        if end_timestamp:
            params["timeLte"] = int(end_timestamp * 1000)
        # 默认只返回 1 W 条数据

        results = []
        end_emp_time = 0
        for i in range(0, 10):
            self.logger.info(f"正在处理 Arkham 实体名称: {entity_name}, 第 {i + 1} 页, chains_str: {chains_str}")
            offset = i * page_size
            params["offset"] = str(offset)
            full_url = f"{url}?{urlencode(params)}"
            rep = await self.retry_requests(
                url=url,
                method="get",
                params=params,
                headers=self._init_headers(full_url)

            )
            if rep.status_code in [404, 403]:
                continue
            res = rep.json()
            transfer = res.get("transfers",[])


            if not transfer:
                self.logger.error(f"Arkham API 返回数据为空,entity_name: {entity_name}, chains_str: {chains_str},结束查询")
                break

            arkham_entities = self.find_arkham_entities(transfer)
            rows = []

            for entity in arkham_entities:
                rows.append({
                    'address': entity.get('address'),
                    'chain': entity.get('chain'),
                    'arkham_entity_tag': self.get_ark_tag(entity),
                })
            self.logger.info(f"Arkham 实体名称: {entity_name}, 第 {i + 1} 页,查询到 {len(rows)}个地址标签")

            if transfer:
                end_emp_time = (datetime.strptime(transfer[-1:][0]['blockTimestamp'],'%Y-%m-%dT%H:%M:%SZ').timestamp()+ 8*60*60+1) * 1000


            results.extend(rows)

        return results, end_emp_time
