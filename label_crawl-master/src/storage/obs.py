import os
import asyncio

from io import BytesIO
from functools import partial
from obs import ObsClient


class TGOBSStorage:
    def __init__(self, conn: dict, *args, **kwargs):
        """
        conn: {
            "connect": {
                "ak": "",
                "sk": "",
                "endpoint": "",
                "default_bucket": "",
            },
            "path": None,
        }
        """
        obs_connect = conn.get("connect")
        self.default_bucket = obs_connect.get("default_bucket")
        self.obs_path = conn.get("path", "tg_images")

        default_kwargs = {"long_conn_mode": True}
        kwargs = {**default_kwargs, **kwargs}
        self.obs = ObsClient(
            access_key_id=obs_connect.get("ak"),
            secret_access_key=obs_connect.get("sk"),
            server=obs_connect.get("endpoint"),
            **kwargs,
        )
        self._loop = asyncio.get_event_loop()

    @staticmethod
    def get_object_name(path: str, obj_name: str,) -> str:
        """
        组装对象名称
        :param path:
        :param obj_name:
        :return:
        """
        obj_path = os.path.join(path, obj_name)
        # OBS 不是真正的路径，所以为了兼容 win 系统，需要转换路径分割符
        if os.altsep:
            obj_path = obj_path.replace(os.sep, os.altsep)
        return obj_path

    async def get_image(self, filename) -> bytes:
        """
        从 obs 获取 image 对象
        :param filename:
        :return:
        """
        resp = await self._loop.run_in_executor(
            None,
            partial(
                self.obs.getObject,
                bucketName=self.default_bucket,
                objectKey=self.get_object_name(self.obs_path, filename),
                loadStreamInMemory=True,
            ),
        )
        if resp.status and resp.status > 300:
            raise Exception(f"Bucket: {self.default_bucket}, Path: {self.obs_path}, Obj: {filename}, Exception: {resp.errorMessage}")

        content = resp.body.buffer
        return content

    async def save_image(self, filename, content: BytesIO):
        """
        将 image 对象保存到 obs
        :param filename: 必须包含文件扩展名，如：123.jpg
        :param content:
        :param mime:
        :return:
        """
        obj_key = self.get_object_name(self.obs_path, filename)
        resp = await self._loop.run_in_executor(
            None,
            partial(
                self.obs.putContent,
                bucketName=self.default_bucket,
                objectKey=obj_key,
                content=content
            ),
        )
        if resp.status and resp.status > 300:
            raise Exception(f"Bucket: {self.default_bucket}, Path: {self.obs_path}, Obj: {filename}, Exception: {resp.errorMessage}")

        return resp
