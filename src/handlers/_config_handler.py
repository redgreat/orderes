#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# @author by wangcw @ 2025
# comment: 客户特殊配置处理器

from elasticsearch import Elasticsearch
from loguru import logger
from typing import Dict, Any
from src.base_processor import BaseProcessor
from src.main import process_extra_json

# 独立的客户特殊配置索引名称
custspecialconfig_index_name = "custspecialconfig"

class ConfigHandler(BaseProcessor):
    """处理basic_custspecialconfig表的事件，存储到独立索引"""
    
    def handle(self, action: str, data: Dict) -> bool:
        """处理客户特殊配置事件并写入独立索引"""
        # 使用配置ID作为文档ID
        doc_id = str(data.get('Id'))
        
        # 如果ConfigValue是JSON字符串，处理为对象
        config_value = data.get('ConfigValue')
        if config_value and isinstance(config_value, str) and (config_value.startswith('{') or config_value.startswith('[')):
            config_value = process_extra_json(config_value)
        
        # 构建客户特殊配置文档结构
        config_data = {
            'Id': doc_id,
            'CustomerId': str(data.get('CustomerId')),
            'CustomerName': data.get('CustomerName'),
            'ConfigType': data.get('ConfigType'),
            'ConfigKey': data.get('ConfigKey'),
            'ConfigValue': config_value,
            'Remark': data.get('Remark'),
            'IsEnabled': data.get('IsEnabled'),
            'CreatedById': data.get('CreatedById'),
            'CreatedAt': data.get('CreatedAt'),
            'UpdatedById': data.get('UpdatedById'),
            'UpdatedAt': data.get('UpdatedAt'),
            'DeletedById': data.get('DeletedById'),
            'DeletedAt': data.get('DeletedAt'),
            'Deleted': data.get('Deleted')
        }
        
        if action == "insert":
            return self._execute_es_custconfig("index", doc_id, config_data)
        elif action == "update":
            return self._execute_es_custconfig("index", doc_id, config_data)  # 使用index替代update，简化处理
        elif action == "delete":
            return self._execute_es_custconfig("delete", doc_id, None)
        else:
            logger.warning(f"未定义的操作类型: {action}")
            return False
    
    def _execute_es_custconfig(self, op_type: str, doc_id: str, doc_body: Dict = None) -> bool:
        """执行ES操作，针对客户特殊配置独立索引"""
        try:
            if op_type == "index":
                self.es_client.index(
                    index=custspecialconfig_index_name,
                    id=doc_id,
                    body=doc_body
                )
                logger.success(f"ES写入CustSpecialConfig成功: 索引={custspecialconfig_index_name}, ID={doc_id}")
                return True
            elif op_type == "delete":
                self.es_client.delete(
                    index=custspecialconfig_index_name,
                    id=doc_id
                )
                logger.success(f"ES删除CustSpecialConfig成功: 索引={custspecialconfig_index_name}, ID={doc_id}")
                return True
            else:
                logger.warning(f"未支持的ES操作: {op_type}")
                return False
        except Exception as e:
            if op_type == "delete" and ("document_missing_exception" in str(e) or "404" in str(e)):
                logger.info(f"ES删除CustSpecialConfig时文档不存在，视为成功: 索引={custspecialconfig_index_name}, ID={doc_id}")
                return True
            else:
                logger.error(f"ES {op_type} CustSpecialConfig失败: 索引={custspecialconfig_index_name}, ID={doc_id}, {str(e)}")
                return False
