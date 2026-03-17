"""
Multi-Chain Address Collector
从各链最新区块收集交易地址，查询标签，合并去重后输出CSV
"""

import asyncio
import csv
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Set, Optional

from blockchain_monitor_sdk import SolanaCollector, TRONCollector, BitcoinCollector
from blockchain_monitor_sdk.collectors.evm_html import EVMHtmlCollector
from blockchain_monitor_sdk.labelers.arkm import ArkmLabeler
from blockchain_monitor_sdk.labelers.oklink import OklinkLabeler
from blockchain_monitor_sdk.config import SDKConfig
from blockchain_monitor_sdk.base import AddressInfo

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MultiChainAddressCollector:
    def __init__(self, output_dir: str = "output", proxy: Dict[str, str] = None):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.config = SDKConfig()
        self.proxy = proxy
    
    async def collect_solana(self, count: int = 100) -> List[AddressInfo]:
        logger.info("=" * 50)
        logger.info("Collecting Solana addresses...")
        
        collector = SolanaCollector(self.config, logger)
        try:
            addresses = await collector.collect_real_time_addresses(count)
            logger.info(f"Solana: collected {len(addresses)} addresses")
            return addresses
        except Exception as e:
            logger.error(f"Solana collection failed: {e}")
            return []
        finally:
            collector.close()
    
    async def collect_tron(self, count: int = 100) -> List[AddressInfo]:
        logger.info("=" * 50)
        logger.info("Collecting TRON addresses...")
        
        collector = TRONCollector(self.config, logger)
        try:
            addresses = await collector.collect_real_time_addresses(count)
            logger.info(f"TRON: collected {len(addresses)} addresses")
            return addresses
        except Exception as e:
            logger.error(f"TRON collection failed: {e}")
            return []
        finally:
            await collector.close()
    
    async def collect_bitcoin(self, count: int = 100) -> List[AddressInfo]:
        logger.info("=" * 50)
        logger.info("Collecting Bitcoin addresses...")
        
        collector = BitcoinCollector(self.config, logger)
        try:
            addresses = await collector.collect_real_time_addresses(count)
            logger.info(f"Bitcoin: collected {len(addresses)} addresses")
            return addresses
        except Exception as e:
            logger.error(f"Bitcoin collection failed: {e}")
            return []
        finally:
            await collector.close()
    
    async def collect_evm(self, count: int = 100, chains: List[str] = None) -> List[AddressInfo]:
        logger.info("=" * 50)
        logger.info("Collecting EVM addresses (via HTML)...")
        
        if chains:
            self.config.evm_chains = chains
        
        collector = EVMHtmlCollector(self.config, logger, proxy=self.proxy)
        try:
            addresses = await collector.collect_real_time_addresses(count)
            logger.info(f"EVM: collected {len(addresses)} addresses")
            return addresses
        except Exception as e:
            logger.error(f"EVM collection failed: {e}")
            return []
        finally:
            await collector.close()
    
    async def collect_all(self, count_per_chain: int = 100, include_evm: bool = True) -> Dict[str, List[AddressInfo]]:
        logger.info("=" * 60)
        logger.info("Starting multi-chain address collection")
        logger.info(f"Target: {count_per_chain} addresses per chain")
        logger.info("=" * 60)
        
        results = {}
        
        tasks = [
            self.collect_solana(count_per_chain),
            self.collect_tron(count_per_chain),
            self.collect_bitcoin(count_per_chain),
        ]
        chain_names = ["solana", "tron", "bitcoin"]
        
        if include_evm:
            tasks.append(self.collect_evm(count_per_chain))
            chain_names.append("evm")
        
        results_list = await asyncio.gather(*tasks, return_exceptions=True)
        
        for i, result in enumerate(results_list):
            if isinstance(result, Exception):
                logger.error(f"{chain_names[i]} collection error: {result}")
                results[chain_names[i]] = []
            else:
                results[chain_names[i]] = result
        
        return results
    
    async def query_labels(
        self,
        addresses: List[AddressInfo],
        query_arkm: bool = True,
        query_oklink: bool = True,
        concurrency: int = 5
    ) -> Dict[str, Dict[str, Optional[str]]]:
        """
        批量查询地址标签
        
        Args:
            addresses: 地址列表
            query_arkm: 是否查询ARKM标签
            query_oklink: 是否查询OKLink标签
            concurrency: 并发数
            
        Returns:
            {address: {"arkm": label, "oklink": label}}
        """
        logger.info("=" * 60)
        logger.info("Starting label query...")
        logger.info(f"Total addresses: {len(addresses)}")
        logger.info("=" * 60)
        
        results = {}
        
        addr_list = [{"address": addr.address, "chain": addr.chain} for addr in addresses]
        
        if query_arkm:
            logger.info("Querying ARKM labels...")
            arkm_labeler = ArkmLabeler(self.config, logger, proxy=self.proxy)
            try:
                arkm_results = await arkm_labeler.get_labels_batch(addr_list, concurrency=concurrency)
                for addr, label in arkm_results.items():
                    if addr not in results:
                        results[addr] = {}
                    results[addr]["arkm"] = label
                logger.info(f"ARKM: found {sum(1 for v in arkm_results.values() if v)} labels")
            except Exception as e:
                logger.error(f"ARKM query failed: {e}")
            finally:
                await arkm_labeler.close()
        
        if query_oklink:
            logger.info("Querying OKLink labels...")
            oklink_labeler = OklinkLabeler(self.config, logger, proxy=self.proxy)
            try:
                oklink_results = await oklink_labeler.get_labels_batch(addr_list, concurrency=concurrency)
                for addr, label in oklink_results.items():
                    if addr not in results:
                        results[addr] = {}
                    results[addr]["oklink"] = label
                logger.info(f"OKLink: found {sum(1 for v in oklink_results.values() if v)} labels")
            except Exception as e:
                logger.error(f"OKLink query failed: {e}")
            finally:
                await oklink_labeler.close()
        
        return results
    
    def save_to_csv(
        self,
        addresses: List[AddressInfo],
        labels: Dict[str, Dict[str, Optional[str]]],
        filename: str = None
    ) -> str:
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"addresses_with_labels_{timestamp}.csv"
        
        filepath = self.output_dir / filename
        
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            fieldnames = [
                'address', 'blockchain', 'chain', 'address_type',
                'transaction_hash', 'value', 'timestamp', 'contract_address',
                'arkm_label', 'oklink_label'
            ]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            for addr_info in addresses:
                addr_labels = labels.get(addr_info.address, {})
                writer.writerow({
                    'address': addr_info.address,
                    'blockchain': addr_info.blockchain,
                    'chain': addr_info.chain,
                    'address_type': addr_info.address_type.value if addr_info.address_type else '',
                    'transaction_hash': addr_info.transaction_hash or '',
                    'value': addr_info.value or '',
                    'timestamp': addr_info.timestamp or '',
                    'contract_address': addr_info.contract_address or '',
                    'arkm_label': addr_labels.get('arkm', '') or '',
                    'oklink_label': addr_labels.get('oklink', '') or '',
                })
        
        logger.info(f"Saved {len(addresses)} addresses to {filepath}")
        return str(filepath)
    
    def print_summary(self, results: Dict[str, List[AddressInfo]], labels: Dict[str, Dict[str, Optional[str]]]):
        print("\n" + "=" * 60)
        print("Collection Summary")
        print("=" * 60)
        
        total = 0
        for chain, addresses in results.items():
            count = len(addresses)
            total += count
            print(f"  {chain.upper():12} : {count:5} addresses")
        
        print("-" * 60)
        print(f"  {'TOTAL':12} : {total:5} addresses")
        
        arkm_count = sum(1 for v in labels.values() if v.get('arkm'))
        oklink_count = sum(1 for v in labels.values() if v.get('oklink'))
        
        print("\n" + "=" * 60)
        print("Label Summary")
        print("=" * 60)
        print(f"  {'ARKM':12} : {arkm_count:5} labels found")
        print(f"  {'OKLINK':12} : {oklink_count:5} labels found")
        print("=" * 60)
    
    async def run(
        self,
        count_per_chain: int = 100,
        include_evm: bool = True,
        query_labels: bool = True,
        query_arkm: bool = True,
        query_oklink: bool = True
    ) -> str:
        start_time = time.time()
        
        results = await self.collect_all(count_per_chain, include_evm)
        
        all_addresses = []
        seen = set()
        
        for chain, addresses in results.items():
            for addr_info in addresses:
                key = (addr_info.address, addr_info.chain)
                if key not in seen:
                    seen.add(key)
                    all_addresses.append(addr_info)
        
        labels = {}
        if query_labels:
            labels = await self.query_labels(
                all_addresses,
                query_arkm=query_arkm,
                query_oklink=query_oklink
            )
        
        self.print_summary(results, labels)
        
        filepath = self.save_to_csv(all_addresses, labels)
        
        elapsed = time.time() - start_time
        logger.info(f"Total time: {elapsed:.2f} seconds")
        
        return filepath


async def main():
    proxy = {
        "http": "http://192.168.1.24:7988",
        "https": "http://192.168.1.24:7988"
    }
    
    collector = MultiChainAddressCollector(
        output_dir="output",
        proxy=proxy
    )
    filepath = await collector.run(
        count_per_chain=100,
        include_evm=True,
        query_labels=True,
        query_arkm=True,
        query_oklink=True
    )
    print(f"\nOutput file: {filepath}")


if __name__ == "__main__":
    asyncio.run(main())
