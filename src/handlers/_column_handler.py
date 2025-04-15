#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# @author by wangcw @ 2025
# comment: 自定义列处理器

from elasticsearch import Elasticsearch
from loguru import logger
from typing import Dict, Any
from src.base_processor import BaseProcessor, index_name

class ColumnHandler(BaseProcessor):
    """处理tb_custcolumn表的事件"""
    def handle(self, action: str, data: Dict) -> bool:
        doc_id = str(data.get('WorkOrderId'))
        
        column_data = {
            'Id': str(data.get('Id')),
            'WorkOrderId': doc_id,
            'TypeCode': data.get('TypeCode'),
            'TypeName': data.get('TypeName'),
            'Value': data.get('Value'),
            'InsertTime': data.get('InsertTime'),
            'Deleted': data.get('Deleted')
        }
        
        if action == "insert":
            doc_body = {
                'ColumnInfo': [column_data]
            }
            return self._execute_es("index", doc_id, doc_body)
        elif action == "update":
            script = {
                "source": """
                    if (ctx._source.ColumnInfo == null) {
                        ctx._source.ColumnInfo = new ArrayList();
                    }
                    def found = false;
                    for (int i=0; i<ctx._source.ColumnInfo.size(); i++) {
                        if (ctx._source.ColumnInfo[i].Id == params.column.Id) {
                            ctx._source.ColumnInfo.set(i, params.column);
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        ctx._source.ColumnInfo.add(params.column);
                    }
                """,
                "lang": "painless",
                "params": {
                    "column": column_data
                }
            }
            try:
                self.es_client.update(
                    index=index_name,
                    id=doc_id,
                    body={"script": script}
                )
                # logger.success(f"ES更新ColumnInfo成功: 索引={index_name}, ID={doc_id}, ColumnID={column_data['Id']}")
                return True
            except Exception as e:
                if "document_missing_exception" in str(e) or "404" in str(e):
                    # logger.success(f"ES更新ColumnInfo时，原信息不存在，自动转为插入操作: 索引={index_name}, ID={doc_id}")
                    doc_body = {
                        'ColumnInfo': [column_data]
                    }
                    return self._execute_es("index", doc_id, doc_body)
                else:
                    logger.error(f"ES更新ColumnInfo失败: 索引={index_name}, ID={doc_id}, {str(e)}")
                    return False
        elif action == "delete":
            script = {
                "source": """
                    if (ctx._source.ColumnInfo != null) {
                        def iterator = ctx._source.ColumnInfo.iterator();
                        while (iterator.hasNext()) {
                            if (iterator.next().Id == params.columnId) {
                                iterator.remove();
                            }
                        }
                    }
                """,
                "lang": "painless",
                "params": {
                    "columnId": str(data.get('Id'))
                }
            }
            try:
                self.es_client.update(
                    index=index_name,
                    id=doc_id,
                    body={"script": script}
                )
                # logger.success(f"ES删除ColumnInfo成功: 索引={index_name}, ID={doc_id}, ColumnID={str(data.get('Id'))}")
                return True
            except Exception as e:
                if "document_missing_exception" in str(e) or "404" in str(e):
                    # logger.success(f"ES删除ColumnInfo时文档不存在，视为成功: 索引={index_name}, ID={doc_id}, ColumnID={str(data.get('Id'))}")
                    return True
                else:
                    logger.error(f"ES删除ColumnInfo失败: 索引={index_name}, ID={doc_id}, {str(e)}")
                    return False
        else:
            logger.warning(f"未定义的操作类型: {action}")
            return False