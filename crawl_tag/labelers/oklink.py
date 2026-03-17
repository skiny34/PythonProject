"""
OKLink地址标签查询器

参考 label_crawl-master/src/spiders/oklink.py 实现
"""
import asyncio
import json
import re
from typing import Any, Dict, List, Optional
import aiohttp

from ..base import BaseLabeler


class OklinkLabeler(BaseLabeler):
    """
    OKLink地址标签查询器
    
    通过OKLink网页API查询地址标签信息
    """
    
    PROXY = {
        "http": "http://192.168.1.24:7988",
        "https": "http://192.168.1.24:7988"
    }
    
    CHAIN_URLS = {
        "ethereum": "https://www.oklink.com/zh-hant/ethereum/address/{address}",
        "eth": "https://www.oklink.com/zh-hant/ethereum/address/{address}",
        "bsc": "https://www.oklink.com/zh-hant/bsc/address/{address}",
        "polygon": "https://www.oklink.com/zh-hans/polygon/address/{address}",
        "arbitrum": "https://www.oklink.com/zh-hans/arbitrum-one/address/{address}",
        "optimism": "https://www.oklink.com/zh-hans/optimism/address/{address}",
        "avalanche": "https://www.oklink.com/zh-hans/avax/address/{address}",
        "zksync": "https://www.oklink.com/zh-hans/zksync-era/address/{address}",
        "tron": "https://www.oklink.com/zh-hant/tron/address/{address}",
        "bitcoin": "https://www.oklink.com/zh-hans/btc/address/{address}",
        "btc": "https://www.oklink.com/zh-hans/btc/address/{address}",
        "solana": "https://www.oklink.com/zh-hans/solana/account/{address}",
        "etc": "https://www.oklink.com/zh-hans/etc/address/{address}",
        "base": "https://www.oklink.com/zh-hans/base/address/{address}",
        "aptos": "https://www.oklink.com/zh-hans/aptos/address/{address}",
    }
    
    def __init__(self, config: Any, logger: Any, rate_limiter: Any = None, proxy: Dict[str, str] = None):
        super().__init__(config, logger, rate_limiter)
        self.api_key = None
        if hasattr(config, "labeler"):
            self.api_key = config.labeler.oklink_api_key
        self.max_retries = 3
        self.retry_delay = 5.0
        self.proxy = proxy or self.PROXY
    
    def _get_headers(self) -> Dict[str, str]:
        """获取请求头"""
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
        }
        
        if self.api_key:
            headers['Ok-Api-Key'] = self.api_key
        
        return headers
    
    def _normalize_chain(self, chain: str) -> str:
        """标准化链名称"""
        chain_lower = chain.lower()
        mapping = {
            "eth": "ethereum",
            "btc": "bitcoin",
        }
        return mapping.get(chain_lower, chain_lower)
    
    def _get_url(self, address: str, chain: str) -> Optional[str]:
        """获取查询URL"""
        chain_normalized = self._normalize_chain(chain)
        url_template = self.CHAIN_URLS.get(chain_normalized)
        
        if not url_template:
            if address.startswith("0x"):
                url_template = self.CHAIN_URLS.get("ethereum")
            elif address.startswith("T"):
                url_template = self.CHAIN_URLS.get("tron")
            elif address.startswith(("1", "3", "bc1")):
                url_template = self.CHAIN_URLS.get("bitcoin")
        
        if url_template:
            return url_template.format(address=address)
        
        return None
    
    async def get_label(self, address: str, chain: str = "") -> Optional[str]:
        """
        获取单个地址的OKLink标签
        
        Args:
            address: 区块链地址
            chain: 链名称
            
        Returns:
            标签字符串或None
        """
        url = self._get_url(address, chain)
        
        if not url:
            self.logger.warning(f"Unknown chain for address: {address}")
            return None
        
        headers = self._get_headers()
        
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
                        self.logger.error(f"OKLink API error: {response.status}")
                        return None
                    
                    html_content = await response.text()
                    label = self._parse_label_from_html(html_content, address)
                    
                    if label:
                        self.logger.info(f"Successfully retrieved OKLink label for {address}: {label}")
                    
                    return label
                    
            except asyncio.TimeoutError:
                self.logger.warning(f"OKLink request timeout for {address}, attempt {attempt + 1}/{self.max_retries}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.retry_delay * (attempt + 1))
            except Exception as e:
                self.logger.error(f"OKLink request error for {address}: {e}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.retry_delay * (attempt + 1))
        
        return None
    
    def _parse_label_from_html(self, html_content: str, address: str) -> Optional[str]:
        """从HTML内容中解析标签"""
        try:
            script_pattern = r'<script\s+id="appState"[^>]*data-id="__app_data_for_ssr__"[^>]*>(.*?)</script>'
            match = re.search(script_pattern, html_content, re.DOTALL)
            
            if match:
                script_content = match.group(1)
                
                try:
                    app_data = json.loads(script_content)
                    tag_maps = (
                        app_data.get("appContext", {})
                        .get("initialProps", {})
                        .get("store", {})
                        .get("pageState", {})
                        .get("tagStore", {})
                        .get("tagMaps", {})
                    )
                    
                    hover_tag = tag_maps.get("hoverEntityTag")
                    if hover_tag:
                        return hover_tag
                    
                    entity_tags = tag_maps.get("entityTags", [])
                    if entity_tags and isinstance(entity_tags, list) and len(entity_tags) > 0:
                        first_tag = entity_tags[0]
                        if isinstance(first_tag, dict):
                            return first_tag.get("tag") or first_tag.get("name")
                        elif isinstance(first_tag, str):
                            return first_tag
                except json.JSONDecodeError:
                    pass
            
            tag_store_pattern = r'"tagStore"\s*:\s*\{[^}]*"tagMaps"\s*:\s*(\{[^}]+\})'
            match = re.search(tag_store_pattern, html_content)
            
            if match:
                try:
                    tag_maps_str = match.group(1)
                    tag_maps = json.loads(tag_maps_str)
                    
                    hover_tag = tag_maps.get("hoverEntityTag")
                    if hover_tag:
                        return hover_tag
                    
                    entity_tag = tag_maps.get("entityTag")
                    if entity_tag:
                        return entity_tag
                    
                    entity_tags = tag_maps.get("entityTags", [])
                    if entity_tags and isinstance(entity_tags, list) and len(entity_tags) > 0:
                        first_tag = entity_tags[0]
                        if isinstance(first_tag, dict):
                            return first_tag.get("text") or first_tag.get("tag") or first_tag.get("name")
                        elif isinstance(first_tag, str):
                            return first_tag
                except (json.JSONDecodeError, Exception):
                    pass
            
            hover_tag_pattern = r'"hoverEntityTag"\s*:\s*"([^"]+)"'
            match = re.search(hover_tag_pattern, html_content)
            
            if match:
                return match.group(1)
            
            entity_tag_pattern = r'"entityTag"\s*:\s*"([^"]+)"'
            match = re.search(entity_tag_pattern, html_content)
            
            if match:
                return match.group(1)
            
            entity_tags_pattern = r'"entityTags"\s*:\s*\[\s*"([^"]+)"'
            match = re.search(entity_tags_pattern, html_content)
            
            if match:
                return match.group(1)
            
            entity_tags_obj_pattern = r'"entityTags"\s*:\s*\[\s*\{[^}]*"text"\s*:\s*"([^"]+)"'
            match = re.search(entity_tags_obj_pattern, html_content)
            
            if match:
                return match.group(1)
            
            return None
            
        except Exception as e:
            self.logger.error(f"Failed to parse OKLink HTML for {address}: {e}")
            return None
    
    async def get_labels_batch(
        self,
        addresses: List[Dict[str, str]],
        concurrency: int = 3,
    ) -> Dict[str, Optional[str]]:
        """
        批量获取地址标签
        
        Args:
            addresses: 地址列表，每个元素包含 address 和 chain
            concurrency: 并发请求数 (建议较低，避免被限流)
            
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
    
    async def get_address_info(self, address: str, chain: str = "") -> Optional[Dict]:
        """
        获取地址详细信息
        
        Args:
            address: 区块链地址
            chain: 链名称
            
        Returns:
            地址信息字典或None
        """
        url = self._get_url(address, chain)
        
        if not url:
            return None
        
        headers = self._get_headers()
        
        try:
            await self._rate_limit_wait()
            
            if self._session is None:
                connector = aiohttp.TCPConnector(ssl=False)
                self._session = aiohttp.ClientSession(connector=connector)
            
            proxy = self.proxy.get("https") if url.startswith("https") else self.proxy.get("http")
            
            async with self._session.get(url, headers=headers, timeout=30, proxy=proxy) as response:
                if response.status != 200:
                    return None
                
                html_content = await response.text()
                return self._parse_address_info_from_html(html_content)
                
        except Exception as e:
            self.logger.error(f"Failed to get address info for {address}: {e}")
            return None
    
    def _parse_address_info_from_html(self, html_content: str) -> Optional[Dict]:
        """从HTML内容中解析地址信息"""
        try:
            script_pattern = r'<script\s+id="appState"[^>]*data-id="__app_data_for_ssr__"[^>]*>(.*?)</script>'
            match = re.search(script_pattern, html_content, re.DOTALL)
            
            if not match:
                return None
            
            script_content = match.group(1)
            
            try:
                app_data = json.loads(script_content)
            except json.JSONDecodeError:
                return None
            
            address_info = (
                app_data.get("appContext", {})
                .get("initialProps", {})
                .get("store", {})
                .get("pageState", {})
                .get("addressDetailStore", {})
                .get("addressDetail", {})
            )
            
            return {
                "balance": address_info.get("balance"),
                "transaction_count": address_info.get("txCount"),
                "first_transaction": address_info.get("firstTxTime"),
                "last_transaction": address_info.get("lastTxTime"),
            }
            
        except Exception as e:
            self.logger.error(f"Failed to parse address info: {e}")
            return None
