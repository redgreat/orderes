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
            'ColumnName': data.get('ColumnName'),
            'ColumnValue': data.get('ColumnValue'),
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
                logger.success(f"ES更新ColumnInfo成功: 索引={index_name}, ID={doc_id}, ColumnID={column_data['Id']}")
                return True
            except Exception as e:
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
                logger.success(f"ES删除ColumnInfo成功: 索引={index_name}, ID={doc_id}, ColumnID={str(data.get('Id'))}")
                return True
            except Exception as e:
                logger.error(f"ES删除ColumnInfo失败: 索引={index_name}, ID={doc_id}, {str(e)}")
                return False
        else:
            logger.warning(f"未定义的操作类型: {action}")
            return False