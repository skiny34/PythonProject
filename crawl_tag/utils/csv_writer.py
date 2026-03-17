"""
CSV输出模块
"""
import csv
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional


class CSVWriter:
    """CSV文件写入器"""
    
    def __init__(
        self,
        output_directory: str = "./output",
        filename_pattern: str = "addresses_{timestamp}.csv",
    ):
        self.output_directory = Path(output_directory)
        self.filename_pattern = filename_pattern
        self.output_directory.mkdir(parents=True, exist_ok=True)
    
    def _generate_filename(self) -> str:
        """生成输出文件名"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return self.filename_pattern.format(timestamp=timestamp)
    
    def _get_fieldnames(self) -> List[str]:
        """获取CSV字段名"""
        return [
            "address",
            "blockchain",
            "chain",
            "address_type",
            "transaction_hash",
            "value",
            "timestamp",
            "contract_address",
            "arkm_label",
            "oklink_label",
            "collected_at",
        ]
    
    def _get_transaction_fieldnames(self) -> List[str]:
        """获取交易CSV字段名"""
        return [
            "block_number",
            "chain",
            "transaction_hash",
            "from_address",
            "to_address",
            "value",
            "from_arkm_label",
            "from_oklink_label",
            "to_arkm_label",
            "to_oklink_label",
            "query_time",
        ]
    
    def write_addresses(
        self,
        addresses: List[Dict[str, Any]],
        output_path: Optional[str] = None,
    ) -> str:
        """
        将地址数据写入CSV文件
        
        Args:
            addresses: 地址数据列表
            output_path: 输出文件路径 (可选)
            
        Returns:
            输出文件的完整路径
        """
        if not addresses:
            raise ValueError("No addresses to write")
        
        if output_path is None:
            filename = self._generate_filename()
            output_path = str(self.output_directory / filename)
        
        fieldnames = self._get_fieldnames()
        
        collected_at = datetime.now().isoformat()
        
        with open(output_path, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            
            for addr_info in addresses:
                row = {
                    "address": addr_info.get("address", ""),
                    "blockchain": addr_info.get("blockchain", ""),
                    "chain": addr_info.get("chain", ""),
                    "address_type": addr_info.get("address_type", ""),
                    "transaction_hash": addr_info.get("transaction_hash", ""),
                    "value": addr_info.get("value", ""),
                    "timestamp": addr_info.get("timestamp", ""),
                    "contract_address": addr_info.get("contract_address", ""),
                    "arkm_label": addr_info.get("arkm_label", ""),
                    "oklink_label": addr_info.get("oklink_label", ""),
                    "collected_at": collected_at,
                }
                
                if addr_info.get("extra_data"):
                    for key, value in addr_info["extra_data"].items():
                        if key not in row:
                            row[key] = value
                
                writer.writerow(row)
        
        return output_path
    
    def write_transactions(
        self,
        transactions: List[Dict[str, Any]],
        labels: Dict[str, Dict[str, Optional[str]]] = None,
        output_path: Optional[str] = None,
    ) -> str:
        """
        将交易数据写入CSV文件
        
        Args:
            transactions: 交易数据列表
            labels: 地址标签映射
            output_path: 输出文件路径 (可选)
            
        Returns:
            输出文件的完整路径
        """
        if not transactions:
            raise ValueError("No transactions to write")
        
        if output_path is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"transactions_{timestamp}.csv"
            output_path = str(self.output_directory / filename)
        
        fieldnames = self._get_transaction_fieldnames()
        query_time = datetime.now().isoformat()
        
        with open(output_path, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            
            for tx in transactions:
                from_addr = tx.get("from_address", "")
                to_addr = tx.get("to_address", "")
                
                from_labels = labels.get(from_addr, {}) if labels else {}
                to_labels = labels.get(to_addr, {}) if labels else {}
                
                row = {
                    "block_number": tx.get("block_number", "N/A"),
                    "chain": tx.get("chain", "unknown"),
                    "transaction_hash": tx.get("transaction_hash", "N/A"),
                    "from_address": from_addr,
                    "to_address": to_addr,
                    "value": tx.get("value", 0),
                    "from_arkm_label": from_labels.get("arkm_label", ""),
                    "from_oklink_label": from_labels.get("oklink_label", ""),
                    "to_arkm_label": to_labels.get("arkm_label", ""),
                    "to_oklink_label": to_labels.get("oklink_label", ""),
                    "query_time": query_time,
                }
                writer.writerow(row)
        
        return output_path
    
    def append_addresses(
        self,
        addresses: List[Dict[str, Any]],
        output_path: str,
    ) -> str:
        """
        追加地址数据到现有CSV文件
        
        Args:
            addresses: 地址数据列表
            output_path: 输出文件路径
            
        Returns:
            输出文件的完整路径
        """
        if not addresses:
            return output_path
        
        collected_at = datetime.now().isoformat()
        
        file_exists = os.path.exists(output_path)
        
        with open(output_path, "a", newline="", encoding="utf-8") as csvfile:
            fieldnames = self._get_fieldnames()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction="ignore")
            
            if not file_exists:
                writer.writeheader()
            
            for addr_info in addresses:
                row = {
                    "address": addr_info.get("address", ""),
                    "blockchain": addr_info.get("blockchain", ""),
                    "chain": addr_info.get("chain", ""),
                    "address_type": addr_info.get("address_type", ""),
                    "transaction_hash": addr_info.get("transaction_hash", ""),
                    "value": addr_info.get("value", ""),
                    "timestamp": addr_info.get("timestamp", ""),
                    "contract_address": addr_info.get("contract_address", ""),
                    "arkm_label": addr_info.get("arkm_label", ""),
                    "oklink_label": addr_info.get("oklink_label", ""),
                    "collected_at": collected_at,
                }
                writer.writerow(row)
        
        return output_path
    
    @staticmethod
    def read_addresses(file_path: str) -> List[Dict[str, Any]]:
        """
        从CSV文件读取地址数据
        
        Args:
            file_path: CSV文件路径
            
        Returns:
            地址数据列表
        """
        addresses = []
        
        with open(file_path, "r", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                addresses.append(dict(row))
        
        return addresses
    
    def merge_files(
        self,
        file_paths: List[str],
        output_path: Optional[str] = None,
    ) -> str:
        """
        合并多个CSV文件
        
        Args:
            file_paths: 要合并的文件路径列表
            output_path: 输出文件路径 (可选)
            
        Returns:
            合并后的文件路径
        """
        all_addresses = []
        
        for file_path in file_paths:
            addresses = self.read_addresses(file_path)
            all_addresses.extend(addresses)
        
        return self.write_addresses(all_addresses, output_path)
