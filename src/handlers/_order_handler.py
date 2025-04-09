#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# @author by wangcw @ 2025
# comment: 工单信息处理器

from elasticsearch import Elasticsearch
from loguru import logger
from typing import Dict, Any
from ..event_processor import EventProcessor, index_name

class OrderHandler(EventProcessor):
    """处理tb_workorderinfo表的事件"""
    def handle(self, action: str, data: Dict) -> bool:
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
            return self._execute_es("delete", doc_id)
        else:
            logger.warning(f"未定义的操作类型: {action}")
            return False