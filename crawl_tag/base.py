"""
基础模块 - 定义抽象基类和通用接口
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional, Any
import asyncio
import time


class AddressType(Enum):
    """地址类型枚举"""
    REAL_TIME = "real_time"
    LARGE_VALUE = "large_value"
    POPULAR_CONTRACT = "popular_contract"


class BlockchainType(Enum):
    """区块链类型枚举"""
    EVM = "evm"
    TRON = "tron"
    BITCOIN = "bitcoin"
    SOLANA = "solana"


@dataclass
class AddressInfo:
    """地址信息数据类"""
    address: str
    blockchain: str
    address_type: AddressType
    chain: Optional[str] = None
    transaction_hash: Optional[str] = None
    value: Optional[float] = None
    timestamp: Optional[float] = None
    contract_address: Optional[str] = None
    arkm_label: Optional[str] = None
    oklink_label: Optional[str] = None
    extra_data: Optional[Dict[str, Any]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        result = {
            "address": self.address,
            "blockchain": self.blockchain,
            "address_type": self.address_type.value,
            "chain": self.chain,
            "transaction_hash": self.transaction_hash,
            "value": self.value,
            "timestamp": self.timestamp,
            "contract_address": self.contract_address,
            "arkm_label": self.arkm_label,
            "oklink_label": self.oklink_label,
        }
        if self.extra_data:
            result.update(self.extra_data)
        return result


class BaseCollector(ABC):
    """收集器基类"""
    
    def __init__(self, config: Any, logger: Any, rate_limiter: Any = None):
        self.config = config
        self.logger = logger
        self.rate_limiter = rate_limiter
        self._session = None
    
    @abstractmethod
    async def collect_real_time_addresses(self, count: int) -> List[AddressInfo]:
        """收集实时交易地址"""
        pass
    
    @abstractmethod
    async def collect_large_value_addresses(self, count: int, threshold: float) -> List[AddressInfo]:
        """收集大额交易地址"""
        pass
    
    @abstractmethod
    async def collect_popular_contract_addresses(self, count: int) -> List[AddressInfo]:
        """收集热门合约交互地址"""
        pass
    
    async def collect_all(self, total_count: int, real_time_ratio: float = 0.3,
                          large_value_ratio: float = 0.4, 
                          popular_contract_ratio: float = 0.3,
                          large_value_threshold: float = 10.0) -> List[AddressInfo]:
        """收集所有类型的地址"""
        real_time_count = int(total_count * real_time_ratio)
        large_value_count = int(total_count * large_value_ratio)
        popular_contract_count = total_count - real_time_count - large_value_count
        
        results = []
        
        tasks = [
            self.collect_real_time_addresses(real_time_count),
            self.collect_large_value_addresses(large_value_count, large_value_threshold),
            self.collect_popular_contract_addresses(popular_contract_count),
        ]
        
        results_list = await asyncio.gather(*tasks, return_exceptions=True)
        
        for i, result in enumerate(results_list):
            if isinstance(result, Exception):
                self.logger.error(f"Collection task {i} failed: {result}")
            else:
                results.extend(result)
        
        return results
    
    async def _rate_limit_wait(self):
        """等待速率限制"""
        if self.rate_limiter:
            await self.rate_limiter.acquire()
    
    async def _make_request(self, method: str, url: str, **kwargs) -> Any:
        """发送HTTP请求（带重试和速率限制）"""
        import aiohttp
        
        max_retries = 3
        retry_delay = 5.0
        
        for attempt in range(max_retries):
            try:
                await self._rate_limit_wait()
                
                if self._session is None:
                    self._session = aiohttp.ClientSession()
                
                async with self._session.request(method, url, **kwargs) as response:
                    if response.status == 429:
                        retry_after = float(response.headers.get("Retry-After", retry_delay))
                        self.logger.warning(f"Rate limited, waiting {retry_after}s")
                        await asyncio.sleep(retry_after)
                        continue
                    
                    if response.status >= 500:
                        raise Exception(f"Server error: {response.status}")
                    
                    if response.status == 200:
                        return await response.json()
                    else:
                        raise Exception(f"Request failed with status {response.status}")
                        
            except asyncio.TimeoutError:
                self.logger.warning(f"Request timeout, attempt {attempt + 1}/{max_retries}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay * (attempt + 1))
            except Exception as e:
                self.logger.error(f"Request error: {e}, attempt {attempt + 1}/{max_retries}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay * (attempt + 1))
                else:
                    raise
        
        raise Exception(f"Max retries exceeded for {url}")
    
    async def close(self):
        """关闭资源"""
        if self._session:
            await self._session.close()
            self._session = None


class BaseLabeler(ABC):
    """标签查询器基类"""
    
    def __init__(self, config: Any, logger: Any, rate_limiter: Any = None):
        self.config = config
        self.logger = logger
        self.rate_limiter = rate_limiter
        self._session = None
    
    @abstractmethod
    async def get_label(self, address: str, chain: str) -> Optional[str]:
        """获取地址标签"""
        pass
    
    async def get_labels_batch(self, addresses: List[Dict[str, str]]) -> Dict[str, Optional[str]]:
        """批量获取标签"""
        results = {}
        
        for addr_info in addresses:
            address = addr_info.get("address")
            chain = addr_info.get("chain", "")
            
            if address:
                try:
                    label = await self.get_label(address, chain)
                    results[address] = label
                except Exception as e:
                    self.logger.error(f"Failed to get label for {address}: {e}")
                    results[address] = None
        
        return results
    
    async def _rate_limit_wait(self):
        """等待速率限制"""
        if self.rate_limiter:
            await self.rate_limiter.acquire()
    
    async def close(self):
        """关闭资源"""
        if self._session:
            await self._session.close()
            self._session = None
