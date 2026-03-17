"""
速率限制器
"""
import asyncio
import time
from dataclasses import dataclass
from typing import Optional


@dataclass
class RateLimitConfig:
    """速率限制配置"""
    requests_per_second: float = 5.0
    burst_limit: int = 10
    cooldown_period: float = 60.0


class RateLimiter:
    """
    令牌桶算法实现的速率限制器
    
    支持以下功能:
    - 每秒请求数限制
    - 突发请求限制
    - 冷却期机制
    """
    
    def __init__(
        self,
        requests_per_second: float = 5.0,
        burst_limit: int = 10,
        cooldown_period: float = 60.0,
    ):
        self.requests_per_second = requests_per_second
        self.burst_limit = burst_limit
        self.cooldown_period = cooldown_period
        
        self._tokens = float(burst_limit)
        self._last_update = time.monotonic()
        self._lock = asyncio.Lock()
        self._request_times: list = []
        self._cooldown_until: Optional[float] = None
    
    async def acquire(self, tokens: int = 1) -> bool:
        """
        获取令牌
        
        Args:
            tokens: 需要的令牌数量
            
        Returns:
            是否成功获取令牌
        """
        async with self._lock:
            now = time.monotonic()
            
            if self._cooldown_until and now < self._cooldown_until:
                wait_time = self._cooldown_until - now
                await asyncio.sleep(wait_time)
                now = time.monotonic()
            
            time_passed = now - self._last_update
            self._tokens = min(
                self.burst_limit,
                self._tokens + time_passed * self.requests_per_second
            )
            self._last_update = now
            
            if self._tokens >= tokens:
                self._tokens -= tokens
                self._request_times.append(now)
                self._cleanup_request_times(now)
                return True
            
            wait_time = (tokens - self._tokens) / self.requests_per_second
            await asyncio.sleep(wait_time)
            
            self._tokens = 0
            self._last_update = time.monotonic()
            self._request_times.append(self._last_update)
            self._cleanup_request_times(self._last_update)
            
            return True
    
    def _cleanup_request_times(self, now: float):
        """清理过期的请求时间记录"""
        cutoff = now - self.cooldown_period
        self._request_times = [t for t in self._request_times if t > cutoff]
        
        if len(self._request_times) >= self.burst_limit * 2:
            self._cooldown_until = now + self.cooldown_period
    
    async def reset(self):
        """重置速率限制器"""
        async with self._lock:
            self._tokens = float(self.burst_limit)
            self._last_update = time.monotonic()
            self._request_times = []
            self._cooldown_until = None
    
    @property
    def available_tokens(self) -> float:
        """当前可用令牌数"""
        now = time.monotonic()
        time_passed = now - self._last_update
        return min(
            self.burst_limit,
            self._tokens + time_passed * self.requests_per_second
        )
    
    @property
    def is_in_cooldown(self) -> bool:
        """是否处于冷却期"""
        if self._cooldown_until is None:
            return False
        return time.monotonic() < self._cooldown_until


class MultiServiceRateLimiter:
    """多服务速率限制器"""
    
    def __init__(self):
        self._limiters: dict = {}
    
    def add_service(
        self,
        service_name: str,
        requests_per_second: float = 5.0,
        burst_limit: int = 10,
        cooldown_period: float = 60.0,
    ):
        """添加服务速率限制"""
        self._limiters[service_name] = RateLimiter(
            requests_per_second=requests_per_second,
            burst_limit=burst_limit,
            cooldown_period=cooldown_period,
        )
    
    def get_limiter(self, service_name: str) -> Optional[RateLimiter]:
        """获取服务的速率限制器"""
        return self._limiters.get(service_name)
    
    async def acquire(self, service_name: str, tokens: int = 1) -> bool:
        """获取指定服务的令牌"""
        limiter = self._limiters.get(service_name)
        if limiter is None:
            return True
        return await limiter.acquire(tokens)
    
    async def reset_all(self):
        """重置所有速率限制器"""
        for limiter in self._limiters.values():
            await limiter.reset()
