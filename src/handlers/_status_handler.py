#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# @author by wangcw @ 2025
# comment: 工单状态处理器

from elasticsearch import Elasticsearch
from loguru import logger
from typing import Dict, Any
from src.base_processor import BaseProcessor, index_name

class StatusHandler(BaseProcessor):
    """处理tb_workorderstatus表的事件，存入StatusInfo嵌套字段"""
    def handle(self, action: str, data: Dict) -> bool:
        doc_id = str(data.get('WorkOrderId'))
        
        status_data = {
            'Id': str(data.get('Id')),
            'WorkOrderId': doc_id,
            'WorkStatus': data.get('WorkStatus'),
            'WorkStatusCode': data.get('WorkStatusCode'),
            'NodeCode': data.get('NodeCode'),
            'StepStatus': data.get('StepStatus'),
            'StepName': data.get('StepName'),
            'PreStepStatus': data.get('PreStepStatus'),
            'PreStepName': data.get('PreStepName'),
            'IfUninstall': data.get('IfUninstall'),
            'TypeStatus': data.get('TypeStatus'),
            'SuspendStatus': data.get('SuspendStatus'),
            'IsSwitch': data.get('IsSwitch'),
            'IsMixPreOrder': data.get('IsMixPreOrder'),
            'ClosePersonName': data.get('ClosePersonName'),
            'ClosePersonCode': data.get('ClosePersonCode'),
            'ClosedAt': data.get('ClosedAt'),
            'IsMigration': data.get('IsMigration'),
            'AuditStatus': data.get('AuditStatus'),
            'Remark': data.get('Remark'),
            'CloseReasonCode': data.get('CloseReasonCode'),
            'CloseReasonName': data.get('CloseReasonName'),
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
                'StatusInfo': [status_data]
            }
            return self._execute_es("index", doc_id, doc_body)
        elif action == "update":
            script = {
                "source": """
                    if (ctx._source.StatusInfo == null) {
                        ctx._source.StatusInfo = new ArrayList();
                    }
                    def found = false;
                    for (int i=0; i<ctx._source.StatusInfo.size(); i++) {
                        if (ctx._source.StatusInfo[i].Id == params.status.Id) {
                            ctx._source.StatusInfo.set(i, params.status);
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        ctx._source.StatusInfo.add(params.status);
                    }
                """,
                "lang": "painless",
                "params": {
                    "status": status_data
                }
            }
            try:
                self.es_client.update(
                    index=index_name,
                    id=doc_id,
                    body={"script": script}
                )
                logger.success(f"ES更新StatusInfo成功: 索引={index_name}, ID={doc_id}, StatusID={status_data['Id']}")
                return True
            except Exception as e:
                logger.error(f"ES更新StatusInfo失败: 索引={index_name}, ID={doc_id}, {str(e)}")
                return False
        elif action == "delete":
            script = {
                "source": """
                    if (ctx._source.StatusInfo != null) {
                        def iterator = ctx._source.StatusInfo.iterator();
                        while (iterator.hasNext()) {
                            if (iterator.next().Id == params.statusId) {
                                iterator.remove();
                            }
                        }
                    }
                """,
                "lang": "painless",
                "params": {
                    "statusId": str(data.get('Id'))
                }
            }
            try:
                self.es_client.update(
                    index=index_name,
                    id=doc_id,
                    body={"script": script}
                )
                logger.success(f"ES删除StatusInfo成功: 索引={index_name}, ID={doc_id}, StatusID={str(data.get('Id'))}")
                return True
            except Exception as e:
                logger.error(f"ES删除StatusInfo失败: 索引={index_name}, ID={doc_id}, {str(e)}")
                return False
        else:
            logger.warning(f"未定义的操作类型: {action}")
            return False