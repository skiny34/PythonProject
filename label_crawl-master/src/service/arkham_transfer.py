import time
from service.base import Base

import pandas as pd


class ArkhamTransfer(Base):

    def __init__(self, config, logger, loop):
        super().__init__(config, logger, loop)
        self.is_running = True

    def format_data(self, data):
        """

        :param data:
        :return:
        """

        # 重复地址过滤
        results = []
        chain_address_tags = {}
        for item in data:
            chain = item.get("chain")
            _addr_tags = chain_address_tags.setdefault(chain,{})
            _addr_tags.update({
                item.get("address"): item.get("arkham_entity_tag")
            })

        for chain,chain_tags in chain_address_tags.items():
            for addr,tag in chain_tags.items():
                results.append({
                    "chain":chain,
                    "address":addr,
                    "tag_name":tag
                })
        return results


    async def run(self):

        filename = './address_tag/arkham_entity.xlsx'
        new_filename = './address_tag/arkham_entity_result.xlsx'
        try:
            df = pd.read_excel(filename)
            # 转为列表字典数据
            data = df.to_dict(orient='records')
            docs = []
            for index, items in enumerate(data):
                chains = items.get('chains')

                entity_name = items.get('entity_name', "")
                start_timestamp = items.get('start_timestamp', 0)
                end_timestamp = items.get('end_timestamp', 0)
                start_tem_time = 0


                while self.is_running is True:
                    start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start_timestamp))
                    end_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end_timestamp))
                    self.logger.info(
                        f"正在处理 链{chains} ,第{index + 1}个 实体名称【{entity_name}】时间段: {start_time}-->{end_time}")
                    results,last_emp_time = await self.arkham_spider.get_transfer_by_time_period(entity_name, start_timestamp, end_timestamp,chains)
                    # 记录最后时间戳是否在结束时间内
                    if not results or not last_emp_time:
                        break

                    if int(last_emp_time) / 1000  >= end_timestamp:
                        break

                    start_timestamp = int(last_emp_time) / 1000
                    new_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(last_emp_time) / 1000))
                    self.logger.info(
                        f"Arkham 实体名称: {entity_name}, 查询到 {len(results)} 个地址标签,最新时间: {new_time}")
                    # 处理重复数据
                    docs.extend(self.format_data(results))

            self.export_to_excel(docs,new_filename)

        except Exception as e:
            raise e
        finally:
            self.logger.info("Arkham Transfer 处理完成。")
