#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# @author by wangcw @ 2025
# comment: 记录信息处理器

from elasticsearch import Elasticsearch
from loguru import logger
from typing import Dict, Any
from ..event_processor import EventProcessor, index_name

class RecordHandler(EventProcessor):
    """处理tb_recordinfo表的事件，存入RecordInfo嵌套字段"""
    def handle(self, action: str, data: Dict) -> bool:
        doc_id = str(data.get('WorkOrderId'))
        record_data = {
            'Id': str(data.get('Id')),
            'WorkOrderId': doc_id,
            'RecordType': data.get('RecordType'),
            'RecordContent': data.get('RecordContent'),
            'CreatedById': data.get('CreatedById'),
            'CreatedAt': data.get('CreatedAt'),
            'UpdatedById': data.get('UpdatedById'),
            'UpdatedAt': data.get('UpdatedAt'),
            'DeletedById': data.get('DeletedById'),
            'DeletedAt': data.get('DeletedAt'),
            'Deleted': data.get('Deleted')
        }
        
        if action == "insert":
            doc_body = {
                'RecordInfo': [record_data]
            }
            return self._execute_es("index", doc_id, doc_body)
        elif action == "update":
            script = {
                "source": """
                    if (ctx._source.RecordInfo == null) {
                        ctx._source.RecordInfo = new ArrayList();
                    }
                    def found = false;
                    for (int i=0; i<ctx._source.RecordInfo.size(); i++) {
                        if (ctx._source.RecordInfo[i].Id == params.record.Id) {
                            ctx._source.RecordInfo.set(i, params.record);
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        ctx._source.RecordInfo.add(params.record);
                    }
                """,
                "lang": "painless",
                "params": {
                    "record": record_data
                }
            }
            try:
                self.es_client.update(
                    index=index_name,
                    id=doc_id,
                    body={"script": script}
                )
                logger.success(f"ES更新RecordInfo成功: 索引={index_name}, ID={doc_id}, RecordID={record_data['Id']}")
                return True
            except Exception as e:
                logger.error(f"ES更新RecordInfo失败: 索引={index_name}, ID={doc_id}, {str(e)}")
                return False
        elif action == "delete":
            script = {
                "source": """
                    if (ctx._source.RecordInfo != null) {
                        def iterator = ctx._source.RecordInfo.iterator();
                        while (iterator.hasNext()) {
                            if (iterator.next().Id == params.recordId) {
                                iterator.remove();
                            }
                        }
                    }
                """,
                "lang": "painless",
                "params": {
                    "recordId": str(data.get('Id'))
                }
            }
            try:
                self.es_client.update(
                    index=index_name,
                    id=doc_id,
                    body={"script": script}
                )
                logger.success(f"ES删除RecordInfo成功: 索引={index_name}, ID={doc_id}, RecordID={str(data.get('Id'))}")
                return True
            except Exception as e:
                logger.error(f"ES删除RecordInfo失败: 索引={index_name}, ID={doc_id}, {str(e)}")
                return False
        else:
            logger.warning(f"未定义的操作类型: {action}")
            return False