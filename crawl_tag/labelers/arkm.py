"""
ARKM地址标签查询器

参考 label_crawl-master/src/spiders/arkham.py 实现
"""
import asyncio
import hashlib
import time
from typing import Any, Dict, List, Optional
import aiohttp

from ..base import BaseLabeler


class ArkmLabeler(BaseLabeler):
    """
    ARKM (Arkham Intelligence) 地址标签查询器
    
    通过Arkham API查询地址标签信息
    """
    
    API_BASE_URL = "https://api.arkm.com"
    
    PROXY = {
        "http": "http://192.168.1.24:7988",
        "https": "http://192.168.1.24:7988"
    }
    
    CHAIN_MAPPING = {
        "ethereum": "ethereum",
        "eth": "ethereum",
        "bsc": "bsc",
        "polygon": "polygon",
        "arbitrum": "arbitrum_one",
        "optimism": "optimism",
        "avalanche": "avalanche",
        "base": "base",
        "tron": "tron",
        "bitcoin": "bitcoin",
        "btc": "bitcoin",
        "solana": "solana",
        "doge": "dogecoin",
        "ton": "ton",
        "mantle": "mantle",
        "linea": "linea",
        "blast": "blast",
        "manta": "manta",
        "flare": "flare",
    }
    
    def __init__(self, config: Any, logger: Any, rate_limiter: Any = None, proxy: Dict[str, str] = None):
        super().__init__(config, logger, rate_limiter)
        self.api_key = None
        if hasattr(config, "labeler"):
            self.api_key = config.labeler.arkm_api_key
        self.max_retries = 3
        self.retry_delay = 5.0
        self.proxy = proxy or self.PROXY
    
    @staticmethod
    def _encrypt_x_payload(api_path: str) -> str:
        """
        加密生成X-Payload请求头
        
        Args:
            api_path: API路径 (如 /intelligence/address/0x...)
            
        Returns:
            加密后的payload字符串
        """
        timestamp = str(int(time.time()))
        tk = "gh67j345kl6hj5k432"
        
        path_without_query = api_path.split("?")[0]
        string_payload = f'{path_without_query}:{timestamp}:{tk}'
        encrypt_payload = hashlib.sha256(string_payload.encode('utf-8')).hexdigest()
        last_string_payload = f'{tk}:{encrypt_payload}'
        last_encrypt_payload = hashlib.sha256(last_string_payload.encode('utf-8')).hexdigest()
        
        return last_encrypt_payload
    
    def _get_headers(self, api_path: str) -> Dict[str, str]:
        """获取请求头"""
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'X-Timestamp': str(int(time.time())),
            'X-Payload': self._encrypt_x_payload(api_path),
        }
        
        if self.api_key:
            headers['Authorization'] = f'Bearer {self.api_key}'
        
        return headers
    
    def _normalize_chain(self, chain: str) -> Optional[str]:
        """标准化链名称"""
        if not chain:
            return None
        return self.CHAIN_MAPPING.get(chain.lower(), chain.lower())
    
    async def get_label(self, address: str, chain: str = "") -> Optional[str]:
        """
        获取单个地址的ARKM标签
        
        Args:
            address: 区块链地址
            chain: 链名称 (可选)
            
        Returns:
            标签字符串或None
        """
        api_path = f"/intelligence/address/{address}"
        url = f"{self.API_BASE_URL}{api_path}"
        
        headers = self._get_headers(api_path)
        
        for attempt in range(self.max_retries):
            try:
                await self._rate_limit_wait()
                
                if self._session is None:
                    connector = aiohttp.TCPConnector(ssl=False)
                    self._session = aiohttp.ClientSession(connector=connector)
                
                proxy = self.proxy.get("https") if url.startswith("https") else self.proxy.get("http")
                
                async with self._session.get(url, headers=headers, timeout=30, proxy=proxy) as response:
                    if response.status in [404, 403]:
                        return None
                    
                    if response.status == 429:
                        retry_after = float(response.headers.get("Retry-After", self.retry_delay))
                        self.logger.warning(f"Rate limited, waiting {retry_after}s")
                        await asyncio.sleep(retry_after)
                        continue
                    
                    if response.status != 200:
                        self.logger.error(f"ARKM API error: {response.status}")
                        return None
                    
                    data = await response.json()
                    label = self._parse_label_response(data, address)
                    
                    if label:
                        self.logger.info(f"Successfully retrieved ARKM label for {address}: {label}")
                    
                    return label
                    
            except asyncio.TimeoutError:
                self.logger.warning(f"ARKM request timeout for {address}, attempt {attempt + 1}/{self.max_retries}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.retry_delay * (attempt + 1))
            except Exception as e:
                self.logger.error(f"ARKM request error for {address}: {e}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.retry_delay * (attempt + 1))
        
        return None
    
    def _parse_label_response(self, data: Dict, address: str) -> Optional[str]:
        """解析标签响应"""
        try:
            if data.get('arkhamEntity'):
                entity_name = data.get('arkhamEntity', {}).get('name', '')
                label_name = data.get('arkhamLabel', {}).get('name', '')
                
                if entity_name and label_name:
                    return f"{entity_name}: {label_name}"
                elif entity_name:
                    return entity_name
                elif label_name:
                    return label_name
            
            elif data.get('arkhamLabel'):
                label_name = data.get('arkhamLabel', {}).get('name', '')
                if label_name:
                    return label_name
            
            return None
            
        except Exception as e:
            self.logger.error(f"Failed to parse ARKM response for {address}: {e}")
            return None
    
    async def get_labels_batch(
        self,
        addresses: List[Dict[str, str]],
        concurrency: int = 5,
    ) -> Dict[str, Optional[str]]:
        """
        批量获取地址标签
        
        Args:
            addresses: 地址列表，每个元素包含 address 和 chain
            concurrency: 并发请求数
            
        Returns:
            地址到标签的映射字典
        """
        results = {}
        semaphore = asyncio.Semaphore(concurrency)
        
        async def _get_label_with_semaphore(addr_info: Dict):
            async with semaphore:
                address = addr_info.get("address", "")
                chain = addr_info.get("chain", "")
                label = await self.get_label(address, chain)
                return address, label
        
        tasks = [_get_label_with_semaphore(addr_info) for addr_info in addresses]
        
        try:
            results_list = await asyncio.gather(*tasks, return_exceptions=True)
            
            for result in results_list:
                if isinstance(result, Exception):
                    self.logger.error(f"Batch label error: {result}")
                else:
                    address, label = result
                    results[address] = label
                    
        except Exception as e:
            self.logger.error(f"Batch label query failed: {e}")
        
        return results
    
    async def get_entity_transfers(
        self,
        entity_name: str,
        start_timestamp: Optional[float] = None,
        end_timestamp: Optional[float] = None,
        chains: str = "all",
        page_size: int = 1000,
        max_pages: int = 10,
    ) -> List[Dict]:
        """
        获取实体的转账记录
        
        Args:
            entity_name: 实体名称
            start_timestamp: 开始时间戳
            end_timestamp: 结束时间戳
            chains: 链名称，逗号分隔或"all"
            page_size: 每页大小
            max_pages: 最大页数
            
        Returns:
            转账记录列表
        """
        results = []
        api_path = "/transfers"
        
        for page in range(max_pages):
            params = {
                "base": entity_name,
                "flow": "all",
                "usdGte": "1",
                "sortKey": "time",
                "sortDir": "asc",
                "limit": str(page_size),
                "offset": str(page * page_size),
            }
            
            if chains != "all":
                chain_list = chains.split(",")
                normalized_chains = [self._normalize_chain(c) for c in chain_list]
                params["chains"] = ",".join(filter(None, normalized_chains))
            
            if start_timestamp:
                params["timeGte"] = int(start_timestamp * 1000)
            
            if end_timestamp:
                params["timeLte"] = int(end_timestamp * 1000)
            
            url = f"{self.API_BASE_URL}{api_path}"
            headers = self._get_headers(api_path)
            
            try:
                await self._rate_limit_wait()
                
                if self._session is None:
                    connector = aiohttp.TCPConnector(ssl=False)
                    self._session = aiohttp.ClientSession(connector=connector)
                
                proxy = self.proxy.get("https")
                
                async with self._session.get(url, headers=headers, params=params, timeout=30, proxy=proxy) as response:
                    if response.status in [404, 403]:
                        break
                    
                    if response.status != 200:
                        self.logger.error(f"ARKM transfers API error: {response.status}")
                        break
                    
                    data = await response.json()
                    transfers = data.get("transfers", [])
                    
                    if not transfers:
                        break
                    
                    for transfer in transfers:
                        address_info = self._extract_address_from_transfer(transfer)
                        if address_info:
                            results.append(address_info)
                    
                    self.logger.info(f"ARKM entity {entity_name}, page {page + 1}: found {len(transfers)} transfers")
                    
            except Exception as e:
                self.logger.error(f"Failed to get transfers for entity {entity_name}: {e}")
                break
        
        return results
    
    def _extract_address_from_transfer(self, transfer: Dict) -> Optional[Dict]:
        """从转账记录中提取地址信息"""
        try:
            if "arkhamEntity" in transfer or "arkhamLabel" in transfer:
                entity_name = transfer.get("arkhamEntity", {}).get("name", "")
                label_name = transfer.get("arkhamLabel", {}).get("name", "")
                
                tag = ""
                if entity_name and label_name:
                    tag = f"{entity_name} {label_name}"
                elif entity_name:
                    tag = entity_name
                elif label_name:
                    tag = label_name
                
                return {
                    "address": transfer.get("address"),
                    "chain": transfer.get("chain"),
                    "arkham_entity_tag": tag,
                }
        except Exception:
            pass
        
        return None
