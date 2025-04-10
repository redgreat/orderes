#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# @author by wangcw @ 2025
# @generate at 2025/3/21 15:26
# comment: 监听mysql binglog，监听变化量后写入宽表

import datetime
import decimal
import sys
import os
import configparser
from loguru import logger
import json
import pymysql
from elasticsearch import Elasticsearch

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)

from event_processor import EventProcessor

# 数据库连接定义
config = configparser.ConfigParser()
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
config_path = os.path.join(project_root, "conf", "db.cnf")
config.read(config_path)

src_host = config.get("source", "host")
src_database = config.get("source", "database").split(',')
src_tables = config.get("source", "tables").split(',')
src_user = config.get("source", "user")
src_password = config.get("source", "password")
src_port = int(config.get("source", "port"))
src_charset = config.get("source", "charset")

# 目标ElasticSearch配置
tar_host = config.get("target", "host")
tar_port = int(config.get("target", "port"))
tar_user = config.get("target", "user")
tar_password = config.get("target", "password")

# 日志配置
logDir = os.path.join(project_root, "log")
if not os.path.exists(logDir):
    os.makedirs(logDir, exist_ok=True)
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

SRC_MYSQL_SETTINGS = {
    "host": src_host,
    "port": src_port,
    "user": src_user,
    "passwd": src_password,
    "charset": src_charset,
}

# ElasticSearch连接配置
ES_SETTINGS = {
    "hosts": [f"http://{tar_host}:{tar_port}"],
    "http_auth": (tar_user, tar_password) if tar_user and tar_password else None,
    "timeout": 30
}


def dict_to_str(value):
    if isinstance(value, datetime.datetime):
        return value.strftime('%Y-%m-%d %H:%M:%S')
    elif isinstance(value, decimal.Decimal):
        return str(value)
    elif isinstance(value, bytes):
        try:
            return value.decode('utf-8')
        except UnicodeDecodeError:
            return value.hex()
    elif isinstance(value, str):
        value = value.strip("'")
        try:
            if value.startswith('{') and value.endswith('}'):
                parsed_json = json.loads(value.replace("'", '"'))
                if isinstance(parsed_json, dict):
                    return {str(k): dict_to_str(v) for k, v in parsed_json.items()}
                return parsed_json
            elif value.startswith('[') and value.endswith(']'):
                return json.loads(value.replace("'", '"'))
        except json.JSONDecodeError:
            pass
        return value
    elif isinstance(value, int):
        return value
    elif isinstance(value, dict):
        return {str(k): dict_to_str(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [dict_to_str(item) for item in value]
    elif value is None:
        return None
    else:
        return f"'{value}'"


def process_bu_json_field(value):
    """专门处理Bu*Json字段，处理字节字符串表示法和Unicode编码"""
    if isinstance(value, bytes):
        try:
            return value.decode('utf-8')
        except UnicodeDecodeError:
            return value.hex()
    
    if isinstance(value, str):
        if value.startswith("b'") and value.endswith("'"):
            value = value[2:-1]
        try:
            parsed_json = json.loads(value.replace("'", '"'))
            if isinstance(parsed_json, dict):
                string_keyed_dict = {}
                for k, v in parsed_json.items():
                    if isinstance(k, bytes):
                        k = k.decode('utf-8') if isinstance(k, bytes) else str(k)
                    string_keyed_dict[k] = v
                return string_keyed_dict
            return parsed_json
        except json.JSONDecodeError:
            try:
                return value.encode('latin-1').decode('unicode_escape')
            except (UnicodeDecodeError, UnicodeEncodeError):
                pass
    return value

def dict_to_json(res_value):
    json_record = {}
    for key, value in res_value.items():
        str_key = key.decode('utf-8') if isinstance(key, bytes) else str(key)
        if str_key not in ['schema', 'action']:
            if str_key == 'BussinessJson' or str_key == 'ExtraJson':
                processed_value = process_bu_json_field(value)
                json_record[str_key] = processed_value
            elif str_key.lower().endswith('json'):
                if isinstance(value, str):
                    try:
                        parsed_json = json.loads(value.strip("'").replace("'", '"'))
                        json_record[str_key] = parsed_json
                        continue
                    except json.JSONDecodeError:
                        pass
                json_record[str_key] = dict_to_str(value)
            else:
                json_record[str_key] = dict_to_str(value)
    
    def ensure_serializable(obj):
        if isinstance(obj, dict):
            return {(k.decode('utf-8') if isinstance(k, bytes) else str(k)): ensure_serializable(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [ensure_serializable(item) for item in obj]
        elif isinstance(obj, bytes):
            try:
                return obj.decode('utf-8')
            except UnicodeDecodeError:
                return obj.hex()
        else:
            return obj
    
    serializable_record = ensure_serializable(json_record)
    
    return json.dumps(serializable_record, ensure_ascii=False, indent=4)


def main():
    stream = BinLogStreamReader(
        connection_settings=SRC_MYSQL_SETTINGS,
        server_id=3,
        blocking=True,  # 持续监听
        only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent],  # 指定只监听某些事件
        only_schemas=src_database,  # 指定只监听某些库（但binlog还是要读取全部）
        only_tables=src_tables,  # 指定监听某些表
        log_file='mysql-bin.122238', # 指定起始binlog文件
        log_pos=423530701  # 指定起始位点
    )

    # 创建ElasticSearch连接
    es_client = Elasticsearch(**ES_SETTINGS)
    
    # 创建统一的事件处理器
    processor = EventProcessor(es_client)
    # 使用上下文管理器
    with processor:
        for binlog_event in stream:
            for row in binlog_event.rows:
                event = {"schema": binlog_event.schema, "table": binlog_event.table}
                
                # 确定事件类型和数据
                if isinstance(binlog_event, WriteRowsEvent):
                    event["action"] = "insert"
                    event.update(row["values"])
                elif isinstance(binlog_event, UpdateRowsEvent):
                    event["action"] = "update"
                    event.update(row["after_values"])
                elif isinstance(binlog_event, DeleteRowsEvent):
                    event["action"] = "delete"
                    event.update(row["values"])
                
                # 转换为JSON数据
                json_data = json.loads(dict_to_json(event))
                # if json_data.get('table')=='tb_workbussinessjsoninfo':
                #     print('json_data: ', json_data)
                #     print('event: ', event)
                # 使用处理器处理事件
                processor.handle_event(
                    action=event["action"],
                    data=json_data
                )
    
    sys.stdout.flush()
    stream.close()
    es_client.close()


if __name__ == "__main__":
    main()
