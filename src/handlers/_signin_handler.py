#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# @author by wangcw @ 2025
# comment: 签到信息处理器

from elasticsearch import Elasticsearch
from loguru import logger
from typing import Dict, Any
from src.base_processor import BaseProcessor, index_name

class SigninHandler(BaseProcessor):
    """处理tb_worksignininfo表的事件，存入SigninInfo嵌套字段"""
    def handle(self, action: str, data: Dict) -> bool:
        doc_id = str(data.get('WorkOrderId'))
        signin_data = {
            'Id': str(data.get('Id')),
            'WorkOrderId': doc_id,
            'OrgCode': data.get('OrgCode'),
            'SignType': data.get('SignType'),
            'SignTime': data.get('SignTime'),
            'SignLng': data.get('SignLng'),
            'SignLat': data.get('SignLat'),
            'SignAddr': data.get('SignAddr'),
            'OriginalAddr': data.get('OriginalAddr'),
            'SignAddrDistance': data.get('SignAddrDistance'),
            'LastSignDistance': data.get('LastSignDistance'),
            'InitialLng': data.get('InitialLng'),
            'InitialLat': data.get('InitialLat'),
            'IMEI': data.get('IMEI'),
            'Remark': data.get('Remark'),
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
                'SigninInfo': [signin_data]
            }
            return self._execute_es("index", doc_id, doc_body)
        elif action == "update":
            script = {
                "source": """
                    if (ctx._source.SigninInfo == null) {
                        ctx._source.SigninInfo = new ArrayList();
                    }
                    def found = false;
                    for (int i=0; i<ctx._source.SigninInfo.size(); i++) {
                        if (ctx._source.SigninInfo[i].Id == params.signin.Id) {
                            ctx._source.SigninInfo.set(i, params.signin);
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        ctx._source.SigninInfo.add(params.signin);
                    }
                """,
                "lang": "painless",
                "params": {
                    "signin": signin_data
                }
            }
            try:
                self.es_client.update(
                    index=index_name,
                    id=doc_id,
                    body={"script": script}
                )
                logger.success(f"ES更新SigninInfo成功: 索引={index_name}, ID={doc_id}, SigninID={signin_data['Id']}")
                return True
            except Exception as e:
                if "document_missing_exception" in str(e) or "404" in str(e):
                    logger.info(f"ES更新SigninInfo时，原信息不存在，自动转为插入操作: 索引={index_name}, ID={doc_id}")
                    doc_body = {
                        'SigninInfo': [signin_data]
                    }
                    return self._execute_es("index", doc_id, doc_body)
                else:
                    logger.error(f"ES更新SigninInfo失败: 索引={index_name}, ID={doc_id}, {str(e)}")
                    return False
        elif action == "delete":
            script = {
                "source": """
                    if (ctx._source.SigninInfo != null) {
                        def iterator = ctx._source.SigninInfo.iterator();
                        while (iterator.hasNext()) {
                            if (iterator.next().Id == params.signinId) {
                                iterator.remove();
                            }
                        }
                    }
                """,
                "lang": "painless",
                "params": {
                    "signinId": str(data.get('Id'))
                }
            }
            try:
                self.es_client.update(
                    index=index_name,
                    id=doc_id,
                    body={"script": script}
                )
                logger.success(f"ES删除SigninInfo成功: 索引={index_name}, ID={doc_id}, SigninID={str(data.get('Id'))}")
                return True
            except Exception as e:
                if "document_missing_exception" in str(e) or "404" in str(e):
                    logger.info(f"ES删除SigninInfo时文档不存在，视为成功: 索引={index_name}, ID={doc_id}, SigninID={str(data.get('Id'))}")
                    return True
                else:
                    logger.error(f"ES删除SigninInfo失败: 索引={index_name}, ID={doc_id}, {str(e)}")
                    return False
        else:
            logger.warning(f"未定义的操作类型: {action}")
            return False