from math import ceil
from sys import platform

import pandas as pd

from service.base import Base



class BlockAddress(Base):

    def __init__(self, config,logger, loop):
        super().__init__(config, logger, loop)

        self.chains = ["ETH"]





    async def run(self):


        # 先查询涉及到多少总量
        # chain_report_count = await self.chain_abuse_spider.curl_platform_abuse_count()
        exports_data = []
        try:
            for chain in self.chains:
                filename = f"./block_address/{chain}_block_address_data.xlsx"
                # reports_count = 0
                # for item in chain_report_count:
                #     kind = item.get('kind', "")
                #     if kind != chain:
                #         continue
                #     reports_count = item.get('reportsFiledCount', 0)
                #
                # # NOTE 因为接口限制每次请求最大50 ，如果还有下一页数据需要做翻页
                # if not reports_count:
                #    continue

                # self.logger.info(f"platform  {chain},abuse data count is {reports_count}.")

                # 翻页请求report 数据
                abuse_data = await self.chain_abuse_spider.abuse_request(chain)
                if not abuse_data:
                    continue
                self.export_to_excel(abuse_data, filename)

                # exports_data.extend(abuse_data)
                self.logger.info(f"platform  {chain},edges data is deal finished.")



        except Exception as e:
            raise e
