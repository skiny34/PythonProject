import asyncio
import json
from copy import deepcopy
from idlelib.rpc import request_queue
from math import ceil

from spiders.spider_common import SpiderCommon


class ChainAbuseSpider(SpiderCommon):

    def __init__(self, config, logger, loop):
        super().__init__(config, logger, loop)

        self.chain_abuse_config = self.spider_config.get("chain_abuse", {})
        self.block_config = self.chain_abuse_config.get("block_address")
        self.report_count_query = self.block_config.get("report_count_query", "")
        self.report_query = self.block_config.get("report_query", "")
        self.report_loss_query = self.block_config.get("report_loss_query", "")
        self.chain_abuse_url = self.block_config.get("url", "")
        self.need_deal_chains = self.block_config.get("chains")

        self.count_abuse_data = {
            "operationName": "GetChainFilterOptions",
            "variables": {
                "input": {}
            },
            "query": self.report_count_query,

        }

        self.report_abuse_data = {
            "operationName": "GetReports",
            "variables": {
                "input": {
                    "chains": [],
                    "orderBy": {
                        "field": "UPVOTES_COUNT",
                        "direction": "DESC"
                    }
                },
                "first": 50
            },
            "query": self.report_query
        }

        self.chains_count_list = []

    async def curl_platform_abuse_count(self):
        """

        :param chain:
        :return:
        """
        self.logger.info(f"get cur abuse count start")
        reports_count = 0
        count_abuse_json = deepcopy(self.count_abuse_data)
        status_code,browse_data_count = await self.fetch_with_retry(self.chain_abuse_url, method="post", is_json=True,
                                                        json=count_abuse_json)

        if status_code != 200:
            self.logger.error(f"cur {self.chain_abuse_url} request error.")
            return None

        abuse_count_res =browse_data_count.get("data", {})

        if not abuse_count_res:
            self.logger.info(f"cur {self.chain_abuse_url} request is empty.")
            return None

        platform_abuse_chains = abuse_count_res.get('chains', [])

        return platform_abuse_chains



    async def abuse_request(self, chain):
        scam_data = []
        page_size = 10
        for i in range(0, page_size):
            page = i + 1
            self.logger.info(f"Chain {chain},开始请求第{page}页数据")
            abuse_json = deepcopy(self.report_abuse_data)
            abuse_json["variables"]["input"]["chains"] = [chain]
            status_code,browse_data = await self.fetch_with_retry(
                self.chain_abuse_url,
                method="post",
                is_json=True,
                json=abuse_json
            )
            if status_code != 200:
                self.logger.error(f"platform  {chain}, cur {page} page  request is error")
                continue
            abuse_data = browse_data.get("data", {})
            page_info = {}
            abuse_reports = {}
            if abuse_data:
                abuse_reports = abuse_data.get('reports', {})
                if abuse_reports:
                    page_info = abuse_reports.get('pageInfo', {})
            else:
                self.logger.info(f"platform  {chain},abuse data is empty.")
            # total_count = abuse_data.get('totalCount', 0)

            self.logger.info(f"platform  {chain},abuse processing cur  {page} page data !!!")
            if page_info.get('hasNextPage'):
                self.logger.info('has next page ,count more than 50')
                end_cursor = page_info.get('endCursor', "")
                self.report_abuse_data['variables'].update({'after': end_cursor})

            self.logger.info(f"platform  {chain}, cur {page} page processed cursior code {end_cursor}")
            # 处理风险事件数据
            edges_data = abuse_reports.get('edges', [])

            for edg in edges_data:
                node_data = edg.get('node')

                vote_count = node_data.get('biDirectionalVoteCount',0)
                if vote_count < 5:
                    continue
                addresses = node_data.get("addresses",[])
                scam_category = node_data.get('scamCategory',"")

                for addr in addresses:
                    _id = node_data.get('id')
                    address = addr.get("address")
                    report_url = node_data.get("domain")
                    if not address and not report_url:
                        continue
                    _chain = addr.get("chain")
                    if address and _chain and _chain != chain:
                        continue


                    created_at_sio = node_data.get('createdAt', "")
                    description = node_data.get('description', "")
                    scam = {
                        "platform": chain,
                        "scam_category": scam_category,
                        "created_at_sio": created_at_sio,
                        "description":description,
                        "report_url": report_url,
                        "address":address
                    }
                    scam_data.append(scam)
            await asyncio.sleep(0.5)
        self.logger.info(f"Chain {chain},查询到涉及点赞超过5次的风险事件数据{len(scam_data)}条")
        return scam_data
        
       
