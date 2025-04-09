#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# @author by wangcw @ 2025
# comment: 服务信息处理器

from elasticsearch import Elasticsearch
from loguru import logger
from typing import Dict, Any
from ..event_processor import EventProcessor, index_name

class ServiceHandler(EventProcessor):
    """处理tb_workserviceinfo表的事件"""
    def handle(self, action: str, data: Dict) -> bool:
        doc_id = str(data.get('WorkOrderId'))
        service_data = {
            'Id': data.get('Id'),
            'WorkOrderId': doc_id,
            'ServiceType': data.get('ServiceType'),
            'AreaType': data.get('AreaType'),
            'Privoder': data.get('Privoder'),
            'InstitutionCode': data.get('InstitutionCode'),
            'IsSelfService': data.get('IsSelfService'),
            'ServiceId': data.get('ServiceId'),
            'ServiceCode': data.get('ServiceCode'),
            'ServiceName': data.get('ServiceName'),
            'WorkerId': data.get('WorkerId'),
            'WorkerCode': data.get('WorkerCode'),
            'WorkerName': data.get('WorkerName'),
            'IsPreInstall': data.get('IsPreInstall'),
            'CarServiceRelation': data.get('CarServiceRelation'),
            'CompleteTime': data.get('CompleteTime'),
            'Remark': data.get('Remark'),
            'CreatedById': data.get('CreatedById'),
            'CreatedAt': data.get('CreatedAt'),
            'UpdatedById': data.get('UpdatedById'),
            'UpdatedAt': data.get('UpdatedAt'),
            'DeletedById': data.get('DeletedById'),
            'DeletedAt': data.get('DeletedAt'),
            'Deleted': data.get('Deleted'),
            'LastUpdateTimeStamp': data.get('LastUpdateTimeStamp')
        }
        if action == "insert":
            doc_body = {
                'ServiceInfo': [service_data]
            }
            return self._execute_es("index", doc_id, doc_body)
        elif action == "update":
            script = {
                "source": """
                    if (ctx._source.ServiceInfo == null) {
                        ctx._source.ServiceInfo = new ArrayList();
                    }
                    def found = false;
                    for (int i=0; i<ctx._source.ServiceInfo.size(); i++) {
                        if (ctx._source.ServiceInfo[i].Id == params.service.Id) {
                            ctx._source.ServiceInfo.set(i, params.service);
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        ctx._source.ServiceInfo.add(params.service);
                    }
                """,
                "lang": "painless",
                "params": {
                    "service": service_data
                }
            }
            try:
                self.es_client.update(
                    index=index_name,
                    id=doc_id,
                    body={"script": script}
                )
                logger.success(f"ES更新ServiceInfo成功: 索引={index_name}, ID={doc_id}, ServiceID={service_data['Id']}")
                return True
            except Exception as e:
                logger.error(f"ES更新ServiceInfo失败: 索引={index_name}, ID={doc_id}, {str(e)}")
                return False
        elif action == "delete":
            script = {
                "source": """
                    if (ctx._source.ServiceInfo != null) {
                        def iterator = ctx._source.ServiceInfo.iterator();
                        while (iterator.hasNext()) {
                            if (iterator.next().Id == params.serviceId) {
                                iterator.remove();
                            }
                        }
                    }
                """,
                "lang": "painless",
                "params": {
                    "serviceId": str(data.get('Id'))
                }
            }
            try:
                self.es_client.update(
                    index=index_name,
                    id=doc_id,
                    body={"script": script}
                )
                logger.success(f"ES删除ServiceInfo成功: 索引={index_name}, ID={doc_id}, ServiceID={str(data.get('Id'))}")
                return True
            except Exception as e:
                logger.error(f"ES删除ServiceInfo失败: 索引={index_name}, ID={doc_id}, {str(e)}")
                return False
        else:
            logger.warning(f"未定义的操作类型: {action}")
            return False