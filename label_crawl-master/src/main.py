import asyncio

from nb_util.logging.helper import get_logger

from config import config
from nb_frame.star.config import LoadConfig
from service.address_tag import AddressTag
from service.arkham_transfer import ArkhamTransfer
from service.baidu_image import BaiduImage
from service.block_address import BlockAddress
from service.wallet_explorer import BtcWalletExplorer

logger = get_logger("Main")


services_hub = {
    "address_tag":AddressTag,
    "block_address":BlockAddress,
    "arkham_transfer":ArkhamTransfer,
    "baidu_images":BaiduImage,
    "wallet_explorer":BtcWalletExplorer,
}

class Main:

    def __init__(self):
        self.loop = asyncio.get_event_loop()
        self.config = LoadConfig.load_config(config)
        self.logger = logger

    def main(self):
        try:
            self.logger.info("Start export...")
            service_config = self.config.get("SERVICE_CONFIG")
            service_type = service_config.get("type", "address_tag")
            service = services_hub.get(service_type)
            loop = asyncio.get_event_loop()
            obj = self.task_run(service, self.config, self.logger, loop)
            loop.run_until_complete(obj)

        except (KeyboardInterrupt, SystemExit):
            self.logger.error("Export error,Exit export...")

    @staticmethod
    async def task_run(service_cls, config, logger, loop):
        services = service_cls(config, logger, loop)
        await services.run()

if __name__ == '__main__':
    main = Main()
    main.main()