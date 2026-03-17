from service.base import Base


class BtcWalletExplorer(Base):

    def __init__(self, config, logger, loop):
        super().__init__(config, logger, loop)





    async def run(self):


        try:

            entity_names = await self.wallet_explorer_spider.get_entity()


            for entity_name in entity_names:

                entity_data = []
                self.logger.info(f"开始查询实体{entity_name}...")

                wallets = await self.wallet_explorer_spider.get_wallet_by_api(entity_name)
            # 先爬去涉及多少个实体


                self.logger.info(f"实体{entity_name},查询钱包数量{len(wallets)}")
                # 过滤标签数据

                if not wallets:
                    self.logger.info(f"实体{entity_name}没有查询到钱包地址，跳过...")
                    continue

                _addresses = list(wallets.keys())

                addresses = await self.filter_tags_addresses("btc_tag_rel", _addresses)

                self.logger.info(f"实体{entity_name},需要入库标签地址数量{len(addresses)}")

                for addr,addr_info in wallets.items():
                    if addr not in addresses:
                        continue
                    entity_data.append(addr_info)


                await self.save_mongo_data(entity_data,coll="wallet_explorer",db="tag_crawl")
                # 存入mongo 数据


        except Exception as e:
            self.logger.error(f"处理地址标签时发生错误: {e}")

        finally:
            self.logger.info("地址标签爬取任务完成!")



