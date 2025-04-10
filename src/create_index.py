#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# @author by wangcw @ 2025
# @generate at 2025-4-9 14:14:09
# comment: 创建ElasticSearch索引结构

from elasticsearch import Elasticsearch
from loguru import logger
import configparser
import os

# 数据库连接定义
config = configparser.ConfigParser()
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
config_path = os.path.join(project_root, "conf", "db.cnf")
config.read(config_path)

# 目标ElasticSearch配置
tar_host = config.get("target", "host")
tar_port = int(config.get("target", "port"))
tar_user = config.get("target", "user")
tar_password = config.get("target", "password")
index_name = config.get("target", "index_name")

# 日志配置
logDir = os.path.expanduser("../log/")
if not os.path.exists(logDir):
    os.mkdir(logDir)
logFile = os.path.join(logDir, "repl.log")
# logger.remove(handler_id=None)

logger.add(
    logFile,
    colorize=True,
    rotation="1 days",
    retention="3 days",
    format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}",
    backtrace=True,
    diagnose=True,
    level="INFO",
)

# ElasticSearch连接配置
ES_SETTINGS = {
    "hosts": [f"http://{tar_host}:{tar_port}"],
    "http_auth": (tar_user, tar_password) if tar_user and tar_password else None,
    "timeout": 30
}

def create_order_index():
    """创建工单索引结构"""
    es = Elasticsearch(**ES_SETTINGS)
    
    # 索引映射定义
    mapping = {
        "mappings": {
            "properties": {
                "Id": {"type": "keyword"},
                "AppCode": {"type": "keyword"},
                "SourceType": {"type": "keyword"},
                "OrderType": {"type": "keyword"},
                "CreateType": {"type": "keyword"},
                "ServiceProviderCode": {"type": "keyword"},
                "WorkStatus": {"type": "keyword"},
                "CustomerId": {"type": "keyword"},
                "CustomerName": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "CustStoreId": {"type": "keyword"},
                "CustStoreName": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "CustStoreCode": {"type": "keyword"},
                "PreCustStoreId": {"type": "keyword"},
                "PreCustStoreName": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "CustSettleId": {"type": "keyword"},
                "CustSettleName": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "IsCustomer": {"type": "text"},
                "CustCoopType": {"type": "keyword"},
                "ProCode": {"type": "keyword"},
                "ProName": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "CityCode": {"type": "keyword"},
                "CityName": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "AreaCode": {"type": "keyword"},
                "AreaName": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "InstallAddress": {"type": "text"},
                "InstallTime": {
                  "type": "date",
                  "format": "yyyy-MM-dd'T'HH:mm:ssxxx||yyyy-MM-dd HH:mm:ss"
                },
                        "RequiredTime": {
                  "type": "date",
                  "format": "yyyy-MM-dd'T'HH:mm:ssxxx||yyyy-MM-dd HH:mm:ss"
                },
                "LinkMan": {"type": "text"},
                "LinkTel": {"type": "keyword"},
                "SecondLinkTel": {"type": "keyword"},
                "SecondLinkMan": {"type": "text"},
                "WarehouseId": {"type": "keyword"},
                "WarehouseName": {"type": "text"},
                "Remark": {"type": "text"},
                "IsUrgent": {"type": "text"},
                "CustUniqueSign": {"type": "keyword"},
                "CreatePersonCode": {"type": "keyword"},
                "CreatePersonName": {"type": "text"},
                "EffectiveTime": {
                  "type": "date",
                  "format": "yyyy-MM-dd'T'HH:mm:ssxxx||yyyy-MM-dd HH:mm:ss"
                },
                "EffectiveSuccessfulTime": {
                  "type": "date",
                  "format": "yyyy-MM-dd'T'HH:mm:ssxxx||yyyy-MM-dd HH:mm:ss"
                },
                "CreatedById": {"type": "keyword"},
                "CreatedAt": {
                  "type": "date",
                  "format": "yyyy-MM-dd'T'HH:mm:ssxxx||yyyy-MM-dd HH:mm:ss"
                },
                "UpdatedById": {"type": "keyword"},
                "UpdatedAt": {
                  "type": "date",
                  "format": "yyyy-MM-dd'T'HH:mm:ssxxx||yyyy-MM-dd HH:mm:ss"
                },
                "DeletedById": {"type": "keyword"},
                "DeletedAt": {
                  "type": "date",
                  "format": "yyyy-MM-dd'T'HH:mm:ssxxx||yyyy-MM-dd HH:mm:ss"
                },
                "Deleted": {"type": "text"},
                "LastUpdateTimeStamp": {
                  "type": "date",
                  "format": "yyyy-MM-dd'T'HH:mm:ssxxx||yyyy-MM-dd HH:mm:ss"
                },
                "StatusInfo": {
                  "type": "nested"
                },
                "CarInfo": {
                  "type": "nested"
                },
                "ServiceInfo": {
                  "type": "nested"
                },
                "RecordInfo": {
                  "type": "nested"
                },
                "AppointInfo": {
                  "type": "nested"
                },
                "AppointConcat": {
                  "type": "nested"
                },
                "OperateInfo": {
                  "type": "nested"
                },
                "JsonInfo": {
                  "type": "nested"
                },
                "ColumnInfo": {
                  "type": "nested"
                },
                "ConfigInfo": {
                  "type": "nested"
                },
                "SignInfo": {
                  "type": "nested"
                }
            }
        }
    }
    
    try:
        # 删除已存在的索引（如果存在）
        if es.indices.exists(index=index_name):
            es.indices.delete(index=index_name)
            logger.info(f"已删除现有索引: {index_name}")
            
        # 创建新索引
        es.indices.create(index=index_name, mappings=mapping["mappings"])
        logger.success(f"成功创建索引: {index_name}")
        return True
    except Exception as e:
        logger.error(f"创建索引失败: {str(e)}")
        return False

if __name__ == "__main__":
    create_order_index()