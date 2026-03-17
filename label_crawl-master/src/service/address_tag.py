import copy

import pandas as pd

from service.base import Base


class AddressTag(Base):

    def __init__(self, config, logger, loop):
        super().__init__(config, logger, loop)
        self.chain = self.config.get("CHAIN_SOURCE","eth")

    async def run(self):


        if self.chain == "eth":

            filename = './address_tag/address_tag_eth.xlsx'
        else:
            filename = './address_tag/address_tag_other.xlsx'

        new_filename = './address_tag/address_tag_result.xlsx'
        df = pd.read_excel(filename)
        # 转为列表字典数据
        data = df.to_dict(orient='records')
        results = []
        platform_tag_map = {}
        try:

            default_dict = {
                'chain': "",
                'address': "",
                "oklink": "",
                "arkham": "",
                "merkle_science": "",
                "database_entity": "",
            }

            for index, items in enumerate(data):

                chain = items.get('chain').lower()
                address = items.get('address', "")
                # address = "0xacd03d601e5bb1b275bb94076ff46ed9d753435a"
                platforms = items.get('platform')
                database_entity = items.get('db_entity')
                _platform_tag = platform_tag_map.setdefault(address, copy.deepcopy(default_dict))
                _platform_tag["database_entity"] = database_entity
                _platform_tag["address"] = address
                _platform_tag["chain"] = chain
                platforms_list = platforms.split(',') if platforms else []
                self.logger.info(
                    f"正在处理 --第{index + 1}个-- 地址【{address}】,需要爬取【{platforms_list}】多个平台标签……")
                for _platform in platforms_list:

                    if _platform == "oklink":
                        address_tag = await self.oklink_spider.address_tag_query(chain=chain, address=address)


                    elif _platform == "arkham":
                        address_tag = await self.arkham_spider.address_tag_query(chain=chain, address=address)


                    elif _platform == "mist_track":

                        address_tag = await self.mist_track_spider.address_tag_query(chain=chain,
                                                                                     address=address)
                    elif _platform == "merkle_science":
                        address_tag = await self.merkle_science_spider.address_tag_query(chain=chain, address=address)

                    elif _platform == "scan":
                        address_tag = await self.scan_spider.address_tag_query(addr=address, chain=chain)

                    else:
                        self.logger.warning(f"未知平台标签：{_platform}，跳过处理。")
                        continue

                    _platform_tag[_platform] = str(address_tag) if address_tag else ""

            results = list(platform_tag_map.values())
        # 存储到 mongo 服务器上跑
            await self.save_mongo_data(results,coll="address_tag",db="tag_database")
            # self.export_to_excel(data=results, filename=new_filename)
        except Exception as e:
            self.logger.error(f"处理地址标签时发生错误: {e}")
        finally:
            self.logger.info("地址标签爬取任务完成!")
