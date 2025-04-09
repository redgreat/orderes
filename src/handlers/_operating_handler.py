#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# @author by wangcw @ 2025
# comment: 操作信息处理器

from elasticsearch import Elasticsearch
from loguru import logger
from typing import Dict, Any
from src.base_processor import BaseProcessor, index_name

class OperatingHandler(BaseProcessor):
    """处理tb_operatinginfo表的事件"""
    def handle(self, action: str, data: Dict) -> bool:
        doc_id = str(data.get('WorkOrderId'))
        
        operating_data = {
            'Id': str(data.get('Id')),
            'WorkOrderId': doc_id,
            'OperatorId': data.get('OperatorId'),
            'OperatorName': data.get('OperatorName'),
            'OperatorCode': data.get('OperatorCode'),
            'OperationType': data.get('OperationType'),
            'OperationName': data.get('OperationName'),
            'OperationTime': data.get('OperationTime'),
            'OperationResult': data.get('OperationResult'),
            'OperationRemark': data.get('OperationRemark'),
            'CreatedAt': data.get('CreatedAt'),
            'CreatedById': data.get('CreatedById'),
            'UpdatedById': data.get('UpdatedById'),
            'UpdatedAt': data.get('UpdatedAt'),
            'DeletedById': data.get('DeletedById'),
            'DeletedAt': data.get('DeletedAt'),
            'Deleted': data.get('Deleted')
        }
        
        if action == "insert":
            doc_body = {
                'OperatingInfo': [operating_data]
            }
            return self._execute_es("index", doc_id, doc_body)
        elif action == "update":
            script = {
                "source": """
                    if (ctx._source.OperatingInfo == null) {
                        ctx._source.OperatingInfo = new ArrayList();
                    }
                    def found = false;
                    for (int i=0; i<ctx._source.OperatingInfo.size(); i++) {
                        if (ctx._source.OperatingInfo[i].Id == params.operating.Id) {
                            ctx._source.OperatingInfo.set(i, params.operating);
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        ctx._source.OperatingInfo.add(params.operating);
                    }
                """,
                "lang": "painless",
                "params": {
                    "operating": operating_data
                }
            }
            try:
                self.es_client.update(
                    index=index_name,
                    id=doc_id,
                    body={"script": script}
                )
                logger.success(f"ES更新OperatingInfo成功: 索引={index_name}, ID={doc_id}, OperatingID={operating_data['Id']}")
                return True
            except Exception as e:
                logger.error(f"ES更新OperatingInfo失败: 索引={index_name}, ID={doc_id}, {str(e)}")
                return False
        elif action == "delete":
            script = {
                "source": """
                    if (ctx._source.OperatingInfo != null) {
                        def iterator = ctx._source.OperatingInfo.iterator();
                        while (iterator.hasNext()) {
                            if (iterator.next().Id == params.operatingId) {
                                iterator.remove();
                            }
                        }
                    }
                """,
                "lang": "painless",
                "params": {
                    "operatingId": str(data.get('Id'))
                }
            }
            try:
                self.es_client.update(
                    index=index_name,
                    id=doc_id,
                    body={"script": script}
                )
                logger.success(f"ES删除OperatingInfo成功: 索引={index_name}, ID={doc_id}, OperatingID={str(data.get('Id'))}")
                return True
            except Exception as e:
                logger.error(f"ES删除OperatingInfo失败: 索引={index_name}, ID={doc_id}, {str(e)}")
                return False
        else:
            logger.warning(f"未定义的操作类型: {action}")
            return False