#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# @author by wangcw @ 2025
# comment: 预约联系人处理器

from elasticsearch import Elasticsearch
from loguru import logger
from typing import Dict, Any
from src.base_processor import BaseProcessor, index_name

class AppointmentConcatHandler(BaseProcessor):
    """处理tb_appointmentconcat表的事件"""
    def handle(self, action: str, data: Dict) -> bool:
        doc_id = str(data.get('WorkOrderId'))
        
        concat_data = {
            'Id': str(data.get('Id')),
            'WorkOrderId': data.get('WorkOrderId'),
            'FirstAppointTime': data.get('FirstAppointTime'),
            'FirstSubmitTime': data.get('FirstSubmitTime'),
            'CorrectiveAppointTime': data.get('CorrectiveAppointTime'),
            'LastRemark': data.get('LastRemark'),
            'AppCode': data.get('AppCode'),
            'AppointStatus': data.get('AppointStatus'),
            'LastAppointTime': data.get('LastAppointTime'),
            'RemarkConcat': data.get('RemarkConcat'),
            'CustRemarkConcat': data.get('CustRemarkConcat'),
            'CallRemarkConcat': data.get('CallRemarkConcat'),
            'ApplyReason': data.get('ApplyReason'),
            'ApplyCode': data.get('ApplyCode'),
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
                'ConcatInfo': [concat_data]
            }
            return self._execute_es("index", doc_id, doc_body)
        elif action == "update":
            script = {
                "source": """
                    if (ctx._source.ConcatInfo == null) {
                        ctx._source.ConcatInfo = new ArrayList();
                    }
                    def found = false;
                    for (int i=0; i<ctx._source.ConcatInfo.size(); i++) {
                        if (ctx._source.ConcatInfo[i].Id == params.concat.Id) {
                            ctx._source.ConcatInfo.set(i, params.concat);
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        ctx._source.ConcatInfo.add(params.concat);
                    }
                """,
                "lang": "painless",
                "params": {
                    "concat": concat_data
                }
            }
            try:
                self.es_client.update(
                    index=index_name,
                    id=doc_id,
                    body={"script": script}
                )
                logger.success(f"ES更新ConcatInfo成功: 索引={index_name}, ID={doc_id}, ConcatID={concat_data['Id']}")
                return True
            except Exception as e:
                if "document_missing_exception" in str(e) or "404" in str(e):
                    logger.info(f"ES更新ConcatInfo时，原信息不存在，自动转为插入操作: 索引={index_name}, ID={doc_id}")
                    doc_body = {
                        'ConcatInfo': [concat_data]
                    }
                    return self._execute_es("index", doc_id, doc_body)
                else:
                    logger.error(f"ES更新ConcatInfo失败: 索引={index_name}, ID={doc_id}, {str(e)}")
                    return False
        elif action == "delete":
            script = {
                "source": """
                    if (ctx._source.ConcatInfo != null) {
                        def iterator = ctx._source.ConcatInfo.iterator();
                        while (iterator.hasNext()) {
                            if (iterator.next().Id == params.concatId) {
                                iterator.remove();
                            }
                        }
                    }
                """,
                "lang": "painless",
                "params": {
                    "concatId": str(data.get('Id'))
                }
            }
            try:
                self.es_client.update(
                    index=index_name,
                    id=doc_id,
                    body={"script": script}
                )
                logger.success(f"ES删除ConcatInfo成功: 索引={index_name}, ID={doc_id}, ConcatID={str(data.get('Id'))}")
                return True
            except Exception as e:
                if "document_missing_exception" in str(e) or "404" in str(e):
                    logger.info(f"ES删除ConcatInfo时文档不存在，视为成功: 索引={index_name}, ID={doc_id}, ConcatID={str(data.get('Id'))}")
                    return True
                else:
                    logger.error(f"ES删除ConcatInfo失败: 索引={index_name}, ID={doc_id}, {str(e)}")
                    return False
        else:
            logger.warning(f"未定义的操作类型: {action}")
            return False