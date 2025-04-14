#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# @author by wangcw @ 2025
# comment: 操作信息处理器

from elasticsearch import Elasticsearch
from loguru import logger
from typing import Dict, Any
from src.base_processor import BaseProcessor

# 独立的操作信息索引名称
operating_index_name = "operating"

class OperatingHandler(BaseProcessor):
    """处理tb_operatinginfo表的事件，存储到独立索引"""
    
    def handle(self, action: str, data: Dict) -> bool:
        """处理操作信息事件并写入独立索引"""
        # 使用操作ID作为文档ID
        doc_id = str(data.get('Id'))
        
        operating_data = {
            'Id': doc_id,
            'WorkOrderId': str(data.get('WorkOrderId')),
            'OperId': data.get('OperId'),
            'AppCode': data.get('AppCode'),
            'OperCode': data.get('OperCode'),
            'OperName': data.get('OperName'),
            'TagType': data.get('TagType'),
            'InsertTime': data.get('InsertTime'),
            'Deleted': data.get('Deleted')
        }
        
        if action == "insert":
            return self._execute_es_operating("index", doc_id, operating_data)
        elif action == "update":
            return self._execute_es_operating("index", doc_id, operating_data)
        elif action == "delete":
            return self._execute_es_operating("delete", doc_id, None)
        else:
            logger.warning(f"未定义的操作类型: {action}")
            return False
    
    def _execute_es_operating(self, op_type: str, doc_id: str, doc_body: Dict = None) -> bool:
        """执行ES操作，针对操作信息独立索引"""
        try:
            if op_type == "index":
                self.es_client.index(
                    index=operating_index_name,
                    id=doc_id,
                    body=doc_body
                )
                logger.success(f"ES写入OperatingInfo成功: 索引={operating_index_name}, ID={doc_id}")
                return True
            elif op_type == "delete":
                self.es_client.delete(
                    index=operating_index_name,
                    id=doc_id
                )
                logger.success(f"ES删除OperatingInfo成功: 索引={operating_index_name}, ID={doc_id}")
                return True
            else:
                logger.warning(f"未支持的ES操作: {op_type}")
                return False
        except Exception as e:
            if op_type == "delete" and ("document_missing_exception" in str(e) or "404" in str(e)):
                logger.info(f"ES删除OperatingInfo时文档不存在，视为成功: 索引={operating_index_name}, ID={doc_id}")
                return True
            else:
                logger.error(f"ES {op_type} OperatingInfo失败: 索引={operating_index_name}, ID={doc_id}, {str(e)}")
                return False
    
    def query_operations_by_work_order(self, work_order_id: str) -> list:
        """通过工单ID查询相关的所有操作记录"""
        try:
            result = self.es_client.search(
                index=operating_index_name,
                body={
                    "query": {
                        "term": {
                            "WorkOrderId": work_order_id
                        }
                    },
                    "sort": [
                        {"InsertTime": {"order": "desc"}}
                    ],
                    "size": 100  # 限制返回数量，可根据需要调整
                }
            )
            
            operations = []
            for hit in result.get('hits', {}).get('hits', []):
                operations.append(hit['_source'])
                
            return operations
        except Exception as e:
            logger.error(f"查询工单操作记录失败: work_order_id={work_order_id}, error={str(e)}")
            return []