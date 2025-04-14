#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# comment: 预约信息处理器

from elasticsearch import Elasticsearch
from loguru import logger
from typing import Dict, Any
from src.base_processor import BaseProcessor, index_name
from src.main import process_extra_json

class AppointHandler(BaseProcessor):
    """处理tb_appointment表的事件"""
    def handle(self, action: str, data: Dict) -> bool:
        doc_id = str(data.get('WorkOrderId'))
        
        # 确保ExtraJson是对象类型
        extra_json = process_extra_json(data.get('ExtraJson'))
        
        appoint_data = {
            'Id': str(data.get('Id')),
            'WorkOrderId': doc_id,
            'AppointType': data.get('AppointType'),
            'AppointTime': data.get('AppointTime'),
            'AppointUseTime': data.get('AppointUseTime'),
            'AppointRemark': data.get('AppointRemark'),
            'IsAbnormal': data.get('IsAbnormal'),
            'ExtraJson': extra_json,
            'Deleted': data.get('Deleted')
        }
        
        if action == "insert":
            doc_body = {
                'AppointInfo': [appoint_data]
            }
            return self._execute_es("index", doc_id, doc_body)
        elif action == "update":
            script = {
                "source": """
                    if (ctx._source.AppointInfo == null) {
                        ctx._source.AppointInfo = new ArrayList();
                    }
                    def found = false;
                    for (int i=0; i<ctx._source.AppointInfo.size(); i++) {
                        if (ctx._source.AppointInfo[i].Id == params.appoint.Id) {
                            ctx._source.AppointInfo.set(i, params.appoint);
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        ctx._source.AppointInfo.add(params.appoint);
                    }
                """,
                "lang": "painless",
                "params": {
                    "appoint": appoint_data
                }
            }
            try:
                self.es_client.update(
                    index=index_name,
                    id=doc_id,
                    body={"script": script}
                )
                logger.success(f"ES更新AppointInfo成功: 索引={index_name}, ID={doc_id}, AppointID={appoint_data['Id']}")
                return True
            except Exception as e:
                if "document_missing_exception" in str(e) or "404" in str(e):
                    logger.info(f"ES更新AppointInfo时，原信息不存在，自动转为插入操作: 索引={index_name}, ID={doc_id}")
                    doc_body = {
                        'AppointInfo': [appoint_data]
                    }
                    return self._execute_es("index", doc_id, doc_body)
                else:
                    logger.error(f"ES更新AppointInfo失败: 索引={index_name}, ID={doc_id}, {str(e)}")
                    return False
        elif action == "delete":
            script = {
                "source": """
                    if (ctx._source.AppointInfo != null) {
                        def iterator = ctx._source.AppointInfo.iterator();
                        while (iterator.hasNext()) {
                            if (iterator.next().Id == params.appointId) {
                                iterator.remove();
                            }
                        }
                    }
                """,
                "lang": "painless",
                "params": {
                    "appointId": str(data.get('Id'))
                }
            }
            try:
                self.es_client.update(
                    index=index_name,
                    id=doc_id,
                    body={"script": script}
                )
                logger.success(f"ES删除AppointInfo成功: 索引={index_name}, ID={doc_id}, AppointID={str(data.get('Id'))}")
                return True
            except Exception as e:
                if "document_missing_exception" in str(e) or "404" in str(e):
                    logger.info(f"ES删除AppointInfo时文档不存在，视为成功: 索引={index_name}, ID={doc_id}, AppointID={str(data.get('Id'))}")
                    return True
                else:
                    logger.error(f"ES删除AppointInfo失败: 索引={index_name}, ID={doc_id}, {str(e)}")
                    return False
        else:
            logger.warning(f"未定义的操作类型: {action}")
            return False 