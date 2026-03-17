import asyncio
import os

import aiohttp
import requests

from spiders.spider_common import SpiderCommon
from tools.tools import download_image


class BaiduImageSpider(SpiderCommon):

    def __init__(self, config, logger, loop):
        super().__init__(config, logger, loop)

        self.baidu_image_config = self.spider_config.get("baidu", {})

        self.baidu_image_url = self.baidu_image_config.get("images_url")

        self.ocr_api = self.config.get("OCR_API")
        self.session_id =   self.baidu_image_config.get("session_id")
        self.sign =   self.baidu_image_config.get("sign")
        self.tk =   self.baidu_image_config.get("tk")
        self.limit = self.baidu_image_config.get("limit",100)








    async def get_baidu_url(self):

        """

        :return:
        """
        results = {}
        params = {
            "carousel": 503,
            "entrance": "GENERAL",
            "extUiData[isLogoShow]": 1,
            "inspire": "general_pc",
            "limit": self.limit,
            "next": 2,
            "render_type": "card",
            "session_id": self.session_id,
            "sign": self.sign,
            "tk":self.tk,
            "tpl_from": "pc",
            "page": 1
        }

        status_code, res = await self.fetch_with_retry(
            url=self.baidu_image_url,
            method="get",
            params=params,
            is_json=True
        )

        if status_code in [404,403]:
            return None

        images_data = res.get("data",{})

        if not images_data:
            self.logger.error("Baidu image data is empty.")
            return None

        images_list = images_data.get("list", [])

        for images in images_list:
            thumb_url = images.get('thumbUrl')
            source_url = images.get("fromUrl")
            if not thumb_url:
                self.logger.error("Baidu image url is empty.")
                continue

            self.logger.info(f"Image URL: {thumb_url}")

            results[thumb_url] = source_url

        return results

    async def download_images(self,images_urls):

        """
        下载图片链接到本地
        :param images_urls:
        :return:
        """
        # file_names = {}
        for images_url in images_urls:
            # filename = f"{images_url.split('/')[-1]}.jpg"
            # file_names[filename] = images_url
            try:
               res = download_image(
                    images_url,
                    save_dir="./images",
                    filename=f"{images_url.split('/')[-1]}.jpg",
                )

               if not res:
                   self.logger.error(f"Failed to download image from {images_url}")

            except Exception as e:
                self.logger.error(f"Error downloading image from {images_url}: {e}")



    async def _images_ocr(self,file_names,file_path,file_name,sp):
        """

        :param file_names:
        :param sp:
        :return:
        """
        async with sp:
            form_data = aiohttp.FormData()
            # 添加文件字段（关键步骤）
            form_data.add_field(
                "file",  # 表单字段名
                open(file_path, 'rb').read(),# 文件对象（二进制模式）

            )
            form_data.add_field(
                "model",
                "PP-OCRv5",

            )

            status_code, res = await self.fetch_with_retry(
                url=self.ocr_api,
                method="post",
                data=form_data,
                is_json=True
            )

            if status_code != 200:
                self.logger.error(f"OCR request failed for {file_path}, status code: {status_code}")
                return None

            res_data = res.get("results", [])
            if not res_data:
                self.logger.info(f"No OCR results for {file_path}")
                return None

            rec_texts = res_data[0].get("rec_texts",[])


            res = {
                    "file_path": file_names.get(file_name),
                    "rec_texts":rec_texts
                }
            self.logger.info(f"{file_name},处理完成！")

            return res




    async def images_ocr(self,file_names,directory='./images/'):
        """
        :return:
        """
        # 读取本地图片进行OCR识别
        # 遍历文件目录
        results = []
        extensions = [".jpg",".png"]

        tasks = []

        for root, dirs, files in os.walk(directory):
            semaphore = asyncio.Semaphore(3)
            for file in files:
                file_path = os.path.join(root, file)
                file_ext = os.path.splitext(file_path)[1].lower()
                file_name = os.path.splitext(file)[0]
                if not file_ext in extensions:
                    continue
                tasks.append(asyncio.create_task(self._images_ocr(file_names,file_path,file_name, semaphore)))
        rep = await asyncio.gather(*tasks)

        return rep




