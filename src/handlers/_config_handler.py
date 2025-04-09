#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# @author by wangcw @ 2025
# comment: 配置信息处理器

from elasticsearch import Elasticsearch
from loguru import logger
from typing import Dict, Any
from ..event_processor import EventProcessor, index_name

class ConfigHandler(EventProcessor):
    """处理tb_config表的事件，存入ConfigInfo嵌套字段"""
    def handle(self, action: str, data: Dict) -> bool:
        doc_id = str(data.get('WorkOrderId'))
        config_data = {
            'Id': str(data.get('Id')),
            'WorkOrderId': doc_id,
            'ConfigType': data.get('ConfigType'),
            'ConfigValue': data.get('ConfigValue'),
            'ConfigRemark': data.get('ConfigRemark'),
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
                'ConfigInfo': [config_data]
            }
            return self._execute_es("index", doc_id, doc_body)
        elif action == "update":
            script = {
                "source": """
                    if (ctx._source.ConfigInfo == null) {
                        ctx._source.ConfigInfo = new ArrayList();
                    }
                    def found = false;
                    for (int i=0; i<ctx._source.ConfigInfo.size(); i++) {
                        if (ctx._source.ConfigInfo[i].Id == params.config.Id) {
                            ctx._source.ConfigInfo.set(i, params.config);
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        ctx._source.ConfigInfo.add(params.config);
                    }
                """,
                "lang": "painless",
                "params": {
                    "config": config_data
                }
            }
            try:
                self.es_client.update(
                    index=index_name,
                    id=doc_id,
                    body={"script": script}
                )
                logger.success(f"ES更新ConfigInfo成功: 索引={index_name}, ID={doc_id}, ConfigID={config_data['Id']}")
                return True
            except Exception as e:
                logger.error(f"ES更新ConfigInfo失败: 索引={index_name}, ID={doc_id}, {str(e)}")
                return False
        elif action == "delete":
            script = {
                "source": """
                    if (ctx._source.ConfigInfo != null) {
                        def iterator = ctx._source.ConfigInfo.iterator();
                        while (iterator.hasNext()) {
                            if (iterator.next().Id == params.configId) {
                                iterator.remove();
                            }
                        }
                    }
                """,
                "lang": "painless",
                "params": {
                    "configId": str(data.get('Id'))
                }
            }
            try:
                self.es_client.update(
                    index=index_name,
                    id=doc_id,
                    body={"script": script}
                )
                logger.success(f"ES删除ConfigInfo成功: 索引={index_name}, ID={doc_id}, ConfigID={str(data.get('Id'))}")
                return True
            except Exception as e:
                logger.error(f"ES删除ConfigInfo失败: 索引={index_name}, ID={doc_id}, {str(e)}")
                return False
        else:
            logger.warning(f"未定义的操作类型: {action}")
            return False