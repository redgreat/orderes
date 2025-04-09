#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# @author by wangcw @ 2025
# @generate at 2025/3/24 09:33
# comment: 事件处理器基类

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError
from loguru import logger
import json
from typing import Dict, Any, Optional

index_name = "orderpy"

class EventProcessor:
    """事件处理器基类，接收JSON数据并根据表名分发到不同的处理方法"""
    def __init__(self, es_client):
        self.es_client = es_client

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def _execute_es(self, operation: str, doc_id: str, body: Optional[Dict[str, Any]] = None) -> bool:
        """执行ElasticSearch操作
        Args:
            operation: 操作类型 (index, update, delete)
            doc_id: 文档ID
            body: 文档内容
        """
        try:
            if operation == "index":
                self.es_client.index(index=index_name, id=doc_id, body=body)
                logger.success(f"ES索引文档成功: 索引={index_name}, ID={doc_id}")
            elif operation == "update":
                try:
                    self.es_client.update(index=index_name, id=doc_id, body={"doc": body})
                    logger.success(f"ES更新文档成功: 索引={index_name}, ID={doc_id}")
                except NotFoundError:
                    # 文档不存在时转为insert操作
                    self.es_client.index(index=index_name, id=doc_id, body=body)
                    logger.success(f"ES文档不存在，转为插入成功: 索引={index_name}, ID={doc_id}")
            elif operation == "delete":
                try:
                    self.es_client.delete(index=index_name, id=doc_id)
                    logger.success(f"ES删除文档成功: 索引={index_name}, ID={doc_id}")
                except NotFoundError:
                    logger.warning(f"ES删除文档失败，文档不存在: 索引={index_name}, ID={doc_id}")
                    return True  # 文档不存在也视为删除成功
            return True
        except Exception as e:
            logger.error(f"ES操作失败: 索引={index_name}, {str(e)}")
            return False

    def handle_event(self, action: str, data: Dict):
        """统一事件处理入口，根据表名分发到不同的处理方法
        Args:
            action: 操作类型 (insert, delete等)
            data: 事件数据，包含表名信息
        """
        # 从数据中获取表名
        table_name = data.get('table', '')
        
        # 根据表名选择处理方法
        if table_name == "tb_workorderinfo":
            return self._handle_order(action, data)
        elif table_name == "tb_workorderstatus":
            return self._handle_status(action, data)
        elif table_name == "tb_workcarinfo":
            return self._handle_car(action, data)
        elif table_name == "tb_workserviceinfo":
            return self._handle_service(action, data)
        elif table_name == "tb_recordinfo":
            return self._handle_record(action, data)
        elif table_name == "tb_appointment":
            return self._handle_appointment(action, data)
        elif table_name == "tb_appointmentconcat":
            return self._handle_concat(action, data)
        elif table_name == "tb_operatinginfo":
            return self._handle_operating(action, data)
        elif table_name == "tb_workbussinessjsoninfo":
            return self._handle_json(action, data)
        elif table_name == "tb_custcolumn":
            return self._handle_column(action, data)
        elif table_name == "basic_custspecialconfig":
            return self._handle_config(action, data)
        elif table_name == "tb_worksignininfo":
            return self._handle_signin(action, data)
        else:
            logger.warning(f"未找到表 {table_name} 对应的处理方法")
            return False

    def _handle_order(self, action: str, data: Dict) -> bool:
        """处理tb_workorderinfo表的事件
        Args:
            action: 操作类型
            data: 事件数据
        """
        doc_id = str(data.get('Id'))
        
        if action == "insert":
            doc_body = {
                'Id': doc_id,
                'AppCode': data.get('AppCode'),
                'SourceType': data.get('SourceType'),
                'OrderType': data.get('OrderType'),
                'CreateType': data.get('CreateType'),
                'ServiceProviderCode': data.get('ServiceProviderCode'),
                'WorkStatus': data.get('WorkStatus'),
                'CustomerId': data.get('CustomerId'),
                'CustomerName': data.get('CustomerName'),
                'CustStoreId': data.get('CustStoreId'),
                'CustStoreName': data.get('CustStoreName'),
                'CustStoreCode': data.get('CustStoreCode'),
                'PreCustStoreId': data.get('PreCustStoreId'),
                'PreCustStoreName': data.get('PreCustStoreName'),
                'CustSettleId': data.get('CustSettleId'),
                'CustSettleName': data.get('CustSettleName'),
                'IsCustomer': data.get('IsCustomer'),
                'CustCoopType': data.get('CustCoopType'),
                'ProCode': data.get('ProCode'),
                'ProName': data.get('ProName'),
                'CityCode': data.get('CityCode'),
                'CityName': data.get('CityName'),
                'AreaCode': data.get('AreaCode'),
                'AreaName': data.get('AreaName'),
                'InstallAddress': data.get('InstallAddress'),
                'InstallTime': data.get('InstallTime'),
                'RequiredTime': data.get('RequiredTime'),
                'LinkMan': data.get('LinkMan'),
                'LinkTel': data.get('LinkTel'),
                'SecondLinkTel': data.get('SecondLinkTel'),
                'SecondLinkMan': data.get('SecondLinkMan'),
                'WarehouseId': data.get('WarehouseId'),
                'WarehouseName': data.get('WarehouseName'),
                'Remark': data.get('Remark'),
                'IsUrgent': data.get('IsUrgent'),
                'CustUniqueSign': data.get('CustUniqueSign'),
                'CreatePersonCode': data.get('CreatePersonCode'),
                'CreatePersonName': data.get('CreatePersonName'),
                'EffectiveTime': data.get('EffectiveTime'),
                'EffectiveSuccessfulTime': data.get('EffectiveSuccessfulTime'),
                'CreatedById': data.get('CreatedById'),
                'CreatedAt': data.get('CreatedAt'),
                'UpdatedById': data.get('UpdatedById'),
                'UpdatedAt': data.get('UpdatedAt'),
                'DeletedById': data.get('DeletedById'),
                'DeletedAt': data.get('DeletedAt'),
                'Deleted': data.get('Deleted'),
                'LastUpdateTimeStamp': data.get('LastUpdateTimeStamp')
            }
            return self._execute_es("index", doc_id, doc_body)
        elif action == "update":
            doc_body = {
                'Id': doc_id,
                'AppCode': data.get('AppCode'),
                'SourceType': data.get('SourceType'),
                'OrderType': data.get('OrderType'),
                'CreateType': data.get('CreateType'),
                'ServiceProviderCode': data.get('ServiceProviderCode'),
                'WorkStatus': data.get('WorkStatus'),
                'CustomerId': data.get('CustomerId'),
                'CustomerName': data.get('CustomerName'),
                'CustStoreId': data.get('CustStoreId'),
                'CustStoreName': data.get('CustStoreName'),
                'CustStoreCode': data.get('CustStoreCode'),
                'PreCustStoreId': data.get('PreCustStoreId'),
                'PreCustStoreName': data.get('PreCustStoreName'),
                'CustSettleId': data.get('CustSettleId'),
                'CustSettleName': data.get('CustSettleName'),
                'IsCustomer': data.get('IsCustomer'),
                'CustCoopType': data.get('CustCoopType'),
                'ProCode': data.get('ProCode'),
                'ProName': data.get('ProName'),
                'CityCode': data.get('CityCode'),
                'CityName': data.get('CityName'),
                'AreaCode': data.get('AreaCode'),
                'AreaName': data.get('AreaName'),
                'InstallAddress': data.get('InstallAddress'),
                'InstallTime': data.get('InstallTime'),
                'RequiredTime': data.get('RequiredTime'),
                'LinkMan': data.get('LinkMan'),
                'LinkTel': data.get('LinkTel'),
                'SecondLinkTel': data.get('SecondLinkTel'),
                'SecondLinkMan': data.get('SecondLinkMan'),
                'WarehouseId': data.get('WarehouseId'),
                'WarehouseName': data.get('WarehouseName'),
                'Remark': data.get('Remark'),
                'IsUrgent': data.get('IsUrgent'),
                'CustUniqueSign': data.get('CustUniqueSign'),
                'CreatePersonCode': data.get('CreatePersonCode'),
                'CreatePersonName': data.get('CreatePersonName'),
                'EffectiveTime': data.get('EffectiveTime'),
                'EffectiveSuccessfulTime': data.get('EffectiveSuccessfulTime'),
                'CreatedById': data.get('CreatedById'),
                'CreatedAt': data.get('CreatedAt'),
                'UpdatedById': data.get('UpdatedById'),
                'UpdatedAt': data.get('UpdatedAt'),
                'DeletedById': data.get('DeletedById'),
                'DeletedAt': data.get('DeletedAt'),
                'Deleted': data.get('Deleted'),
                'LastUpdateTimeStamp': data.get('LastUpdateTimeStamp')
            }
            return self._execute_es("update", doc_id, doc_body)
        elif action == "delete":
            # 使用script删除特定StatusInfo项
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

    def _handle_status(self, action: str, data: Dict) -> bool:
        """处理tb_workorderstatus表的事件，存入StatusInfo嵌套字段
        Args:
            action: 操作类型
            data: 事件数据
        """
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

    def _handle_car(self, action: str, data: Dict) -> bool:
        """处理tb_workcarinfo表的事件，存入CarInfo嵌套字段
        Args:
            action: 操作类型
            data: 事件数据
        """
        doc_id = str(data.get('WorkOrderId'))
        car_data = {
            'Id': str(data.get('Id')),
            'WorkOrderId': doc_id,
            'VinNumber': data.get('VinNumber'),
            'PlateNumber': data.get('PlateNumber'),
            'PlateColor': data.get('PlateColor'),
            'EngineNumber': data.get('EngineNumber'),
            'CarModelId': data.get('CarModelId'),
            'CarModelName': data.get('CarModelName'),
            'CarSeriesId': data.get('CarSeriesId'),
            'CarSeriesName': data.get('CarSeriesName'),
            'CarBrandId': data.get('CarBrandId'),
            'CarBrandName': data.get('CarBrandName'),
            'CarFullName': data.get('CarFullName'),
            'Color': data.get('Color'),
            'CarPrice': data.get('CarPrice'),
            'IsNewCar': data.get('IsNewCar'),
            'CarType': data.get('CarType'),
            'UserName': data.get('UserName'),
            'UserTel': data.get('UserTel'),
            'UserCityCode': data.get('UserCityCode'),
            'UserCityName': data.get('UserCityName'),
            'UserAddress': data.get('UserAddress'),
            'Remark': data.get('Remark'),
            'ShortVin': data.get('ShortVin'),
            'ShortFourVin': data.get('ShortFourVin'),
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
                'CarInfo': [car_data]
            }
            return self._execute_es("index", doc_id, doc_body)
        elif action == "update":
            script = {
                "source": """
                    if (ctx._source.CarInfo == null) {
                        ctx._source.CarInfo = new ArrayList();
                    }
                    def found = false;
                    for (int i=0; i<ctx._source.CarInfo.size(); i++) {
                        if (ctx._source.CarInfo[i].Id == params.car.Id) {
                            ctx._source.CarInfo.set(i, params.car);
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        ctx._source.CarInfo.add(params.car);
                    }
                """,
                "lang": "painless",
                "params": {
                    "car": car_data
                }
            }
            try:
                self.es_client.update(
                    index=index_name,
                    id=doc_id,
                    body={"script": script}
                )
                logger.success(f"ES更新CarInfo成功: 索引={index_name}, ID={doc_id}, CarID={car_data['Id']}")
                return True
            except Exception as e:
                logger.error(f"ES更新CarInfo失败: 索引={index_name}, ID={doc_id}, {str(e)}")
                return False
        elif action == "delete":
            script = {
                "source": """
                    if (ctx._source.CarInfo != null) {
                        def iterator = ctx._source.CarInfo.iterator();
                        while (iterator.hasNext()) {
                            if (iterator.next().Id == params.carId) {
                                iterator.remove();
                            }
                        }
                    }
                """,
                "lang": "painless",
                "params": {
                    "carId": str(data.get('Id'))
                }
            }
            try:
                self.es_client.update(
                    index=index_name,
                    id=doc_id,
                    body={"script": script}
                )
                logger.success(f"ES删除CarInfo成功: 索引={index_name}, ID={doc_id}, CarID={str(data.get('Id'))}")
                return True
            except Exception as e:
                logger.error(f"ES删除CarInfo失败: 索引={index_name}, ID={doc_id}, {str(e)}")
                return False
        else:
            logger.warning(f"未定义的操作类型: {action}")
            return False

    def _handle_service(self, action: str, data: Dict) -> bool:
        """处理tb_workserviceinfo表的事件
        Args:
            action: 操作类型
            data: 事件数据
        """
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

    def _handle_record(self, action: str, data: Dict) -> bool:
        """处理RecordInfo节点的增删改查操作"""
        index_name = "orderpy"
        work_order_id = data.get("WorkOrderId")
        record_id = data.get("Id")
        
        if not work_order_id or not record_id:
            self.logger.error("缺少WorkOrderId或Id字段")
            return False
            
        record_data = {
            "Id": record_id,
            "WorkOrderId": work_order_id,
            "CompleteTime": data.get("CompleteTime"),
            "RecordPersonCode": data.get("RecordPersonCode"),
            "RecordPersonName": data.get("RecordPersonName"),
            "Remark": data.get("Remark"),
            "InsertTime": data.get("InsertTime"),
            "Deleted": data.get("Deleted")
        }
        
        try:
            if action == "delete":
                # 删除RecordInfo节点中的记录
                script = {
                    "script": {
                        "source": "ctx._source.RecordInfo.removeIf(item -> item.Id == params.record_id)",
                        "lang": "painless",
                        "params": {"record_id": record_id}
                    }
                }
                self.es.update(index=index_name, id=work_order_id, body=script)
                self.logger.info(f"已删除RecordInfo记录: {record_id}")
                return True
                
            elif action in ["insert", "update"]:
                # 更新或插入RecordInfo节点
                script = {
                    "script": {
                        "source": """
                            if (ctx._source.RecordInfo == null) {
                                ctx._source.RecordInfo = [params.record_data]
                            } else {
                                def index = ctx._source.RecordInfo.findIndexOf(item -> item.Id == params.record_data.Id)
                                if (index != -1) {
                                    ctx._source.RecordInfo.set(index, params.record_data)
                                } else {
                                    ctx._source.RecordInfo.add(params.record_data)
                                }
                            }
                        """,
                        "lang": "painless",
                        "params": {"record_data": record_data}
                    }
                }
                self.es.update(index=index_name, id=work_order_id, body=script)
                self.logger.info(f"已{'更新' if action == 'update' else '添加'}RecordInfo记录: {record_id}")
                return True
                
        except Exception as e:
            self.logger.error(f"处理RecordInfo失败: {str(e)}")
            return False

    def _handle_appointment(self, action: str, data: Dict) -> bool:
        """处理AppointInfo节点的增删改查操作"""
        
        doc_id = str(data.get('WorkOrderId'))

        appoint_info = {
            'Id': str(data.get('Id')),
            'WorkOrderId': doc_id,
            'AppCode': data.get('AppCode'),
            'AppointStatus': data.get('AppointStatus'),
            'AppointSource': data.get('AppointSource'),
            'OrderTime': data.get('OrderTime'),
            'AppointTime': data.get('AppointTime'),
            'OperatorCode': data.get('OperatorCode'),
            'OperatorName': data.get('OperatorName'),
            'FailCode': data.get('FailCode'),
            'FailText': data.get('FailText'),
            'ApplyReason': data.get('ApplyReason'),
            'ApplyCode': data.get('ApplyCode'),
            'ProCode': data.get('ProCode'),
            'ProName': data.get('ProName'),
            'CityCode': data.get('CityCode'),
            'CityName': data.get('CityName'),
            'AreaCode': data.get('AreaCode'),
            'AreaName': data.get('AreaName'),
            'NextContactTime': data.get('NextContactTime'),
            'InstallAddress': data.get('InstallAddress'),
            'ChangeRemark': data.get('ChangeRemark'),
            'Remark': data.get('Remark'),
            'ExtraJson': data.get('ExtraJson'),
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
                'AppointInfo': [appoint_info]
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
                    "appoint": appoint_info
                }
            }
            try:
                self.es_client.update(
                    index=index_name,
                    id=doc_id,
                    body={"script": script}
                )
                logger.success(f"ES更新AppointInfo成功: 索引={index_name}, ID={doc_id}, AppointID={appoint_info['Id']}")
                return True
            except Exception as e:
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
                logger.error(f"ES删除AppointInfo失败: 索引={index_name}, ID={doc_id}, {str(e)}")
                return False

    def _handle_concat(self, action: str, data: Dict) -> bool:
        """处理tb_appointmentconcat表的事件，存入AppointConcat嵌套字段
        Args:
            action: 操作类型
            data: 事件数据
        """
        doc_id = str(data.get('WorkOrderId'))
        
        concat_data = {
            'Id': str(data.get('Id')),
            'WorkOrderId': doc_id,
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
                'AppointConcat': [concat_data]
            }
            return self._execute_es("index", doc_id, doc_body)
        elif action == "update":
            # 使用script更新特定AppointConcat项
            script = {
                "source": """
                    if (ctx._source.AppointConcat == null) {
                        ctx._source.AppointConcat = new ArrayList();
                    }
                    def found = false;
                    for (int i=0; i<ctx._source.AppointConcat.size(); i++) {
                        if (ctx._source.AppointConcat[i].Id == params.concat.Id) {
                            ctx._source.AppointConcat.set(i, params.concat);
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        ctx._source.AppointConcat.add(params.concat);
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
                logger.success(f"ES更新AppointConcat成功: 索引={index_name}, ID={doc_id}, ConcatID={concat_data['Id']}")
                return True
            except Exception as e:
                logger.error(f"ES更新AppointConcat失败: 索引={index_name}, ID={doc_id}, {str(e)}")
                return False
        elif action == "delete":
            # 使用script删除特定AppointConcat项
            script = {
                "source": """
                    if (ctx._source.AppointConcat != null) {
                        def iterator = ctx._source.AppointConcat.iterator();
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
                logger.success(f"ES删除AppointConcat成功: 索引={index_name}, ID={doc_id}, ConcatID={str(data.get('Id'))}")
                return True
            except Exception as e:
                logger.error(f"ES删除AppointConcat失败: 索引={index_name}, ID={doc_id}, {str(e)}")
                return False
        else:
            logger.warning(f"未定义的操作类型: {action}")
            return False

    def _handle_operating(self, action: str, data: Dict) -> bool:
        """处理tb_operatinginfo表的事件
        Args:
            action: 操作类型
            data: 事件数据
        """
        doc_id = str(data.get('WorkOrderId'))
        operating_data = {
            'Id': data.get('Id'),
            'WorkOrderId': doc_id,
            'OperId': data.get('OperId'),
            'AppCode': data.get('AppCode'),
            'OperCode': data.get('OperCode'),
            'OperName': data.get('OperName'),
            'TagType': data.get('TagType'),
            'InsertTime': data.get('InsertTime'),
            'Deleted': data.get('Deleted')
        }
        if action == "insert":
            doc_body = {
                'OperateInfo': [operating_data]
            }
            return self._execute_es("index", doc_id, doc_body)
        elif action == "update":
            script = {
                "source": """
                    if (ctx._source.OperateInfo == null) {
                        ctx._source.OperateInfo = new ArrayList();
                    }
                    def found = false;
                    for (int i=0; i<ctx._source.OperateInfo.size(); i++) {
                        if (ctx._source.OperateInfo[i].Id == params.operating.Id) {
                            ctx._source.OperateInfo.set(i, params.operating);
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        ctx._source.OperateInfo.add(params.operating);
                    }
                """,
                "lang": "painless",
                "params": {
                    "operating": operating_data
                }
            }
            try:
                self.es_client.update(
                    index=index_name,
                    id=doc_id,
                    body={"script": script}
                )
                logger.success(f"ES更新OperateInfo成功: 索引={index_name}, ID={doc_id}, OperatingID={operating_data['Id']}")
                return True
            except Exception as e:
                logger.error(f"ES更新OperateInfo失败: 索引={index_name}, ID={doc_id}, {str(e)}")
                return False
        elif action == "delete":
            script = {
                "source": """
                    if (ctx._source.OperateInfo != null) {
                        def iterator = ctx._source.OperateInfo.iterator();
                        while (iterator.hasNext()) {
                            if (iterator.next().Id == params.operatingId) {
                                iterator.remove();
                            }
                        }
                    }
                """,
                "lang": "painless",
                "params": {
                    "operatingId": str(data.get('Id'))
                }
            }
            try:
                self.es_client.update(
                    index=index_name,
                    id=doc_id,
                    body={"script": script}
                )
                logger.success(f"ES删除OperateInfo成功: 索引={index_name}, ID={doc_id}, OperatingID={str(data.get('Id'))}")
                return True
            except Exception as e:
                logger.error(f"ES删除OperateInfo失败: 索引={index_name}, ID={doc_id}, {str(e)}")
                return False
        else:
            logger.warning(f"未定义的操作类型: {action}")
            return False

    def _handle_json(self, action: str, data: Dict) -> bool:
        """处理tb_workbussinessjsoninfo表的事件，存入JsonInfo嵌套字段
        Args:
            action: 操作类型
            data: 事件数据
        """
        doc_id = str(data.get('WorkOrderId'))
        
        json_data = {
            'Id': str(data.get('Id')),
            'WorkOrderId': doc_id,
            'BussinessJson': data.get('BussinessJson'),
            'InsertTime': data.get('InsertTime'),
            'Deleted': data.get('Deleted')
        }
        
        if action == "insert":
            doc_body = {
                'JsonInfo': [json_data]
            }
            return self._execute_es("index", doc_id, doc_body)
        elif action == "update":
            # 使用script更新特定JsonInfo项
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
            # 使用script删除特定JsonInfo项
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

    def _handle_column(self, action: str, data: Dict) -> bool:
        """处理tb_custcolumn表的事件，存入ColumnInfo嵌套字段
        Args:
            action: 操作类型
            data: 事件数据
        """
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
            # 使用script更新特定ColumnInfo项
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
            # 使用script删除特定ColumnInfo项
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

    def _handle_config(self, action: str, data: Dict) -> bool:
        """处理basic_custspecialconfig表的事件，存入ConfigInfo嵌套字段
        Args:
            action: 操作类型
            data: 事件数据
        """
        doc_id = str(data.get('CustomerId')) or str(data.get('CustStoreId'))
        
        config_data = {
            'Id': str(data.get('Id')),
            'CustomerId': str(data.get('CustomerId')),
            'CustomerName': data.get('CustomerName'),
            'CustStoreId': str(data.get('CustStoreId')),
            'CustStoreName': data.get('CustStoreName'),
            'ConfirmType': data.get('ConfirmType'),
            'CreatedById': data.get('CreatedById'),
            'CreatedAt': data.get('CreatedAt'),
            'UpdatedById': data.get('UpdatedById'),
            'UpdatedAt': data.get('UpdatedAt'),
            'DeletedById': data.get('DeletedById'),
            'DeletedAt': data.get('DeletedAt'),
            'Deleted': data.get('Deleted'),
            'ExtraJson': data.get('ExtraJson')
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
        
        if action == "insert":
            doc_body = {
                'StatusInfo': [status_data]
            }
            return self._execute_es("index", doc_id, doc_body)
        elif action == "update":
            # 使用script更新特定StatusInfo项
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
            # 使用script删除特定StatusInfo项
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
            try :
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

    def _handle_signin(self, action: str, data: Dict) -> bool:
        """处理tb_worksignininfo表的事件，存入SignInfo嵌套字段
        Args:
            action: 操作类型
            data: 事件数据
        """
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
            'CreatedAt': data.get('CreatedAt'),
            'CreatedById': data.get('CreatedById'),
            'UpdatedById': data.get('UpdatedById'),
            'UpdatedAt': data.get('UpdatedAt'),
            'DeletedById': data.get('DeletedById'),
            'DeletedAt': data.get('DeletedAt'),
            'Deleted': data.get('Deleted')
        }
        
        try:
            if action == "delete":
                # 删除特定签到记录
                script = {
                    "script": {
                        "source": "ctx._source.SignInfo.removeIf(item -> item.Id == params.sign_id)",
                        "lang": "painless",
                        "params": {"sign_id": signin_data['Id']}
                    }
                }
                self.es.update(index="orderpy", id=doc_id, body=script)
                logger.success(f"成功删除签到记录: {signin_data['Id']}")
            else:
                # 更新或插入签到记录
                script = {
                    "script": {
                        "source": """
                            if (ctx._source.SignInfo == null) {
                                ctx._source.SignInfo = [params.sign_data]
                            } else {
                                def existing = ctx._source.SignInfo.find(item -> item.Id == params.sign_data.Id);
                                if (existing != null) {
                                    ctx._source.SignInfo.remove(existing);
                                }
                                ctx._source.SignInfo.add(params.sign_data);
                            }
                        """,
                        "lang": "painless",
                        "params": {"sign_data": signin_data}
                    }
                }
                self.es.update(index="orderpy", id=doc_id, body=script)
                logger.success(f"成功处理签到记录: {signin_data['Id']}")
            return True
        except Exception as e:
            logger.error(f"处理签到记录失败: {str(e)}")
            return False



