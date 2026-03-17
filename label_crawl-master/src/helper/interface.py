from urllib import parse

import aiohttp

class Interface:
    def __init__(self):
        """

        :param domain: api server domain
        """


        self._session = None
        self.running = True

    @property
    def session(self):
        if self._session is None:
            self._session = aiohttp.ClientSession()
        return self._session

    async def request(self,method,url,headers,is_json=False, **kwargs):
        """
        获取token，执行请求
        :param method:
        :param url:
        :param headers:
        :param is_json:
        :param kwargs:
        :return:
        """
        try:
            async with self.session.request(method, url, headers=headers,timeout=60, **kwargs) as resp:
                if is_json:
                    return resp.status, await resp.json()
                else:
                    return resp.status, await resp.text()
        except Exception as e:
            # 处理连接错误和超时
            raise Exception("Connection failed") from e

    async def get(self, path, **kwargs):
        """
        HTTP GET
        :param path:
        :param kwargs:
        :return:
        """
        return await self.request('get', path, **kwargs)

    async def post(self, path, **kwargs):
        """
        HTTP POST
        :param path:
        :param kwargs:
        :return:
        """
        return await self.request('post', path, **kwargs)

    async def close(self):
        """

        :return:
        """
        # if self.running:
        await self.session.close()
        # self.running = False
