#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# @author by wangcw @ 2025
# comment: JSON业务信息处理器

from elasticsearch import Elasticsearch
from loguru import logger
from typing import Dict, Any
from src.base_processor import BaseProcessor, index_name

class JsonHandler(BaseProcessor):
    """处理tb_workbussinessjsoninfo表的事件"""
    def handle(self, action: str, data: Dict) -> bool:
        doc_id = str(data.get('WorkOrderId'))
        
        json_data = {
            'Id': str(data.get('Id')),
            'WorkOrderId': doc_id,
            'JsonType': data.get('JsonType'),
            'JsonContent': data.get('JsonContent'),
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
                'JsonInfo': [json_data]
            }
            return self._execute_es("index", doc_id, doc_body)
        elif action == "update":
            script = {
                "source": """
                    if (ctx._source.JsonInfo == null) {
                        ctx._source.JsonInfo = new ArrayList();
                    }
                    def found = false;
                    for (int i=0; i<ctx._source.JsonInfo.size(); i++) {
                        if (ctx._source.JsonInfo[i].Id == params.json.Id) {
                            ctx._source.JsonInfo.set(i, params.json);
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        ctx._source.JsonInfo.add(params.json);
                    }
                """,
                "lang": "painless",
                "params": {
                    "json": json_data
                }
            }
            try:
                self.es_client.update(
                    index=index_name,
                    id=doc_id,
                    body={"script": script}
                )
                logger.success(f"ES更新JsonInfo成功: 索引={index_name}, ID={doc_id}, JsonID={json_data['Id']}")
                return True
            except Exception as e:
                logger.error(f"ES更新JsonInfo失败: 索引={index_name}, ID={doc_id}, {str(e)}")
                return False
        elif action == "delete":
            script = {
                "source": """
                    if (ctx._source.JsonInfo != null) {
                        def iterator = ctx._source.JsonInfo.iterator();
                        while (iterator.hasNext()) {
                            if (iterator.next().Id == params.jsonId) {
                                iterator.remove();
                            }
                        }
                    }
                """,
                "lang": "painless",
                "params": {
                    "jsonId": str(data.get('Id'))
                }
            }
            try:
                self.es_client.update(
                    index=index_name,
                    id=doc_id,
                    body={"script": script}
                )
                logger.success(f"ES删除JsonInfo成功: 索引={index_name}, ID={doc_id}, JsonID={str(data.get('Id'))}")
                return True
            except Exception as e:
                logger.error(f"ES删除JsonInfo失败: 索引={index_name}, ID={doc_id}, {str(e)}")
                return False
        else:
            logger.warning(f"未定义的操作类型: {action}")
            return False