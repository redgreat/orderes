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

# 所有字段通用的日期格式定义
DATE_FORMAT = "strict_date_optional_time||yyyy-MM-dd'T'HH:mm:ssxxx||yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis"

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
                  "format": DATE_FORMAT,
                  "ignore_malformed": True
                },
                "RequiredTime": {
                  "type": "date",
                  "format": DATE_FORMAT,
                  "ignore_malformed": True
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
                  "format": DATE_FORMAT,
                  "ignore_malformed": True
                },
                "EffectiveSuccessfulTime": {
                  "type": "date",
                  "format": DATE_FORMAT,
                  "ignore_malformed": True
                },
                "CreatedById": {"type": "keyword"},
                "CreatedAt": {
                  "type": "date",
                  "format": DATE_FORMAT,
                  "ignore_malformed": True
                },
                "UpdatedById": {"type": "keyword"},
                "UpdatedAt": {
                  "type": "date",
                  "format": DATE_FORMAT,
                  "ignore_malformed": True
                },
                "DeletedById": {"type": "keyword"},
                "DeletedAt": {
                  "type": "date",
                  "format": DATE_FORMAT,
                  "ignore_malformed": True
                },
                "Deleted": {"type": "text"},
                "LastUpdateTimeStamp": {
                  "type": "date",
                  "format": DATE_FORMAT,
                  "ignore_malformed": True
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
                "ConcatInfo": {
                  "type": "nested"
                },
                "OperatingInfo": {
                  "type": "nested"
                },
                "JsonInfo": {
                  "type": "nested"
                },
                "ColumnInfo": {
                  "type": "nested"
                },
                "SigninInfo": {
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

def create_operating_info_index(operating_index_name):
    """创建OperatingInfo专用索引结构"""
    es = Elasticsearch(**ES_SETTINGS)
    
    # OperatingInfo索引映射定义
    mapping = {
        "mappings": {
            "properties": {
                "Id": {"type": "keyword"},
                "WorkOrderId": {"type": "keyword"},
                "AppCode": {"type": "keyword"},
                "OperId": {"type": "keyword"},
                "OperCode": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "OperName": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "TagType": {"type": "text"},
                "InsertTime": {
                    "type": "date",
                    "format": DATE_FORMAT,
                    "ignore_malformed": True
                },
                "Deleted": {"type": "text"}
            }
        }
    }
    
    try:
        # 删除已存在的索引（如果存在）
        if es.indices.exists(index=operating_index_name):
            es.indices.delete(index=operating_index_name)
            logger.info(f"已删除现有索引: {operating_index_name}")
            
        # 创建新索引
        es.indices.create(index=operating_index_name, mappings=mapping["mappings"])
        logger.success(f"成功创建索引: {operating_index_name}")
        return True
    except Exception as e:
        logger.error(f"创建索引失败: {str(e)}")
        return False

def create_custspecialconfig_index(custspecialconfig_index_name):
    """创建客户特殊配置专用索引结构"""
    es = Elasticsearch(**ES_SETTINGS)
    
    # CustSpecialConfig索引映射定义
    mapping = {
        "mappings": {
            "properties": {
                "Id": {"type": "keyword"},
                "CustomerId": {"type": "keyword"},
                "CustomerName": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "ConfigType": {"type": "keyword"},
                "ConfigKey": {"type": "keyword"},
                "ConfigValue": {"type": "text"},
                "Remark": {"type": "text"},
                "IsEnabled": {"type": "boolean"},
                "CreatedById": {"type": "keyword"},
                "CreatedAt": {
                    "type": "date",
                    "format": DATE_FORMAT,
                    "ignore_malformed": True
                },
                "UpdatedById": {"type": "keyword"},
                "UpdatedAt": {
                    "type": "date",
                    "format": DATE_FORMAT,
                    "ignore_malformed": True
                },
                "DeletedById": {"type": "keyword"},
                "DeletedAt": {
                    "type": "date",
                    "format": DATE_FORMAT,
                    "ignore_malformed": True
                },
                "Deleted": {"type": "boolean"}
            }
        }
    }
    
    try:
        # 删除已存在的索引（如果存在）
        if es.indices.exists(index=custspecialconfig_index_name):
            es.indices.delete(index=custspecialconfig_index_name)
            logger.info(f"已删除现有索引: {custspecialconfig_index_name}")
            
        # 创建新索引
        es.indices.create(index=custspecialconfig_index_name, mappings=mapping["mappings"])
        logger.success(f"成功创建索引: {custspecialconfig_index_name}")
        return True
    except Exception as e:
        logger.error(f"创建索引失败: {str(e)}")
        return False

if __name__ == "__main__":
    # 创建主工单索引
    create_order_index()
    
    # 创建OperatingInfo专用索引，手动指定索引名称
    operating_index_name = "operating"
    create_operating_info_index(operating_index_name)
    
    # 创建CustSpecialConfig专用索引，手动指定索引名称
    custspecialconfig_index_name = "custspecialconfig"
    create_custspecialconfig_index(custspecialconfig_index_name)