#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# @author by wangcw @ 2025
# @generate at 2025-4-10 14:23:19
# comment: 根据时间范围初始化工单数据到ElasticSearch

import sys
import os
import argparse
import datetime
import configparser
import json
import pymysql
from loguru import logger
from elasticsearch import Elasticsearch

# 添加项目根目录到系统路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 导入事件处理器
from event_processor import EventProcessor
from main import dict_to_json

# 配置文件读取
config = configparser.ConfigParser()
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
config_path = os.path.join(project_root, "conf", "db.cnf")
config.read(config_path)

# 数据库连接配置
src_host = config.get("source", "host")
src_database = config.get("source", "database")
src_user = config.get("source", "user")
src_password = config.get("source", "password")
src_port = int(config.get("source", "port"))
src_charset = config.get("source", "charset")

# 目标ElasticSearch配置
tar_host = config.get("target", "host")
tar_port = int(config.get("target", "port"))
tar_user = config.get("target", "user")
tar_password = config.get("target", "password")

# 日志级别
log_level = config.get("log", "level")

# 日志配置
logDir = os.path.join(project_root, "log")
if not os.path.exists(logDir):
    os.makedirs(logDir, exist_ok=True)
logFile = os.path.join(logDir, "etl_init.log")

logger.add(
    logFile,
    colorize=True,
    rotation="1 days",
    retention="7 days",
    format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}",
    backtrace=True,
    diagnose=True,
    level=log_level,
)

DB_SETTINGS = {
    "host": src_host,
    "port": src_port,
    "user": src_user,
    "password": src_password,
    "database": src_database,
    "charset": src_charset,
}

ES_SETTINGS = {
    "hosts": [f"http://{tar_host}:{tar_port}"],
    "http_auth": (tar_user, tar_password) if tar_user and tar_password else None,
    "timeout": 30
}


def process_table(conn, cursor, processor, table_name, sql_query, order_ids, batch_size=100):
    """
    通用方法：处理指定表的数据并初始化到ElasticSearch
    
    Args:
        conn: 数据库连接
        cursor: 数据库游标
        processor: 事件处理器
        table_name: 表名
        sql_query: SQL查询语句，可以包含 {id_placeholder} 占位符，如无则进行全量查询
        order_ids: 工单ID列表
        batch_size: 每批处理的记录数，默认为100
        
    Returns:
        int: 成功处理的记录数
    """
    processed_count = 0
    
    # 检查SQL查询是否包含占位符
    if "{id_placeholder}" in sql_query:
        # 按工单ID批次处理
        total_ids = len(order_ids)
        logger.info(f"开始处理表 {table_name}，共有 {total_ids} 个工单ID")
        
        for i in range(0, total_ids, batch_size):
            batch_ids = order_ids[i:i+batch_size]
            id_placeholder = ", ".join(["%s"] * len(batch_ids))
            current_sql = sql_query.format(id_placeholder=id_placeholder)
            
            try:
                cursor.execute(current_sql, batch_ids)
                records = cursor.fetchall()
                
                for record in records:
                    event = {
                        "schema": src_database,
                        "table": table_name,
                        "action": "update"
                    }
                    event.update(record)
                    
                    json_data = json.loads(dict_to_json(event))
                    
                    result = processor.handle_event(
                        action="update",
                        data=json_data
                    )
                    
                    if result:
                        processed_count += 1
                        if processed_count % 50 == 0:
                            logger.info(f"表 {table_name} 已处理 {processed_count} 条记录")
            
            except Exception as e:
                logger.error(f"处理表 {table_name} 批次数据时发生错误: {str(e)}")
    else:
        # 全量查询处理
        logger.info(f"开始处理表 {table_name} 的全量数据")
        try:
            cursor.execute(sql_query)
            records = cursor.fetchall()
            total_records = len(records)
            logger.info(f"表 {table_name} 查询到 {total_records} 条记录")
            
            for record in records:
                event = {
                    "schema": src_database,
                    "table": table_name,
                    "action": "update"
                }
                event.update(record)
                
                json_data = json.loads(dict_to_json(event))
                
                result = processor.handle_event(
                    action="update",
                    data=json_data
                )
                
                if result:
                    processed_count += 1
                    if processed_count % 50 == 0:
                        logger.info(f"表 {table_name} 已处理 {processed_count} 条记录")
        
        except Exception as e:
            logger.error(f"处理表 {table_name} 全量数据时发生错误: {str(e)}")
    
    logger.success(f"表 {table_name} 处理完成，共处理 {processed_count} 条记录")
    return processed_count


def init_data(start_time, end_time=None, batch_size=100):
    """
    根据时间范围初始化工单数据到ElasticSearch
    
    Args:
        start_time: 开始时间，格式为 YYYY-MM-DD HH:MM:SS
        end_time: 结束时间，格式为 YYYY-MM-DD HH:MM:SS
        batch_size: 每批处理的记录数，默认为100
    """
    if end_time is None:
        end_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
    logger.info(f"开始初始化数据，时间范围: {start_time} 至 {end_time}")
    
    try:
        conn = pymysql.connect(**DB_SETTINGS)
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        logger.info("数据库连接成功")
    except Exception as e:
        logger.error(f"数据库连接失败: {str(e)}")
        return None, None
    
    try:
        es_client = Elasticsearch(**ES_SETTINGS)
        logger.info("ElasticSearch连接成功")
    except Exception as e:
        logger.error(f"ElasticSearch连接失败: {str(e)}")
        cursor.close()
        conn.close()
        return
    
    processor = EventProcessor(es_client)
    
    try:
        id_sql = """
        SELECT Id 
        FROM tb_workorderinfo 
        WHERE CreatedAt BETWEEN %s AND %s
        ORDER BY Id
        """
        cursor.execute(id_sql, (start_time, end_time))
        id_records = cursor.fetchall()
        
        if not id_records:
            logger.warning("没有找到符合条件的工单数据")
            return
        
        order_ids = [str(record['Id']) for record in id_records]
        total_count = len(order_ids)
        logger.info(f"找到符合条件的工单数: {total_count}")
        
        tables_to_process = {
            "tb_workorderinfo": """
            SELECT * 
            FROM tb_workorderinfo 
            WHERE Id IN ({id_placeholder})
            """,
            "tb_workorderstatus": """
            SELECT * 
            FROM tb_workorderstatus 
            WHERE WorkOrderId IN ({id_placeholder})
            """,
            "tb_appointmentconcat": """
            SELECT * 
            FROM tb_appointmentconcat 
            WHERE WorkOrderId IN ({id_placeholder})
            """,
            "tb_appointment": """
            SELECT * 
            FROM tb_appointment 
            WHERE WorkOrderId IN ({id_placeholder})
            """,
            "tb_workcarinfo": """
            SELECT * 
            FROM tb_workcarinfo 
            WHERE WorkOrderId IN ({id_placeholder})
            """,
            "tb_custcolumn": """
            SELECT * 
            FROM tb_custcolumn 
            WHERE WorkOrderId IN ({id_placeholder})
            """,
            "basic_custspecialconfig": """
            SELECT * 
            FROM basic_custspecialconfig
            """,
            "tb_workbussinessjsoninfo": """
            SELECT Id, WorkOrderId, BussinessJson, InsertTime, Deleted
            FROM tb_workbussinessjsoninfo 
            WHERE WorkOrderId IN ({id_placeholder})
            """,
            "tb_operatinginfo": """
            SELECT * 
            FROM tb_operatinginfo 
            WHERE WorkOrderId IN ({id_placeholder})
            """,
            "tb_recordinfo": """
            SELECT * 
            FROM tb_recordinfo 
            WHERE WorkOrderId IN ({id_placeholder})
            """,
            "tb_workserviceinfo": """
            SELECT * 
            FROM tb_workserviceinfo 
            WHERE WorkOrderId IN ({id_placeholder})
            """,
            "tb_worksignininfo": """
            SELECT * 
            FROM tb_worksignininfo 
            WHERE WorkOrderId IN ({id_placeholder})
            """
        }
        
        total_processed = 0
        for table_name, sql_query in tables_to_process.items():
            processed = process_table(
                conn, cursor, processor, 
                table_name, sql_query, 
                order_ids, batch_size
            )
            total_processed += processed
        
        logger.success(f"数据初始化完成，共处理 {total_processed} 条记录")
        
        try:
            cursor.execute("SHOW MASTER STATUS")
            binlog_status = cursor.fetchone()
            if binlog_status and len(binlog_status) >= 2:
                log_file = binlog_status['File']
                log_pos = binlog_status['Position']
                logger.info(f"当前binlog位置: {log_file}:{log_pos}")
                return log_file, log_pos
            else:
                logger.warning("无法获取当前binlog位置")
                return None, None
        except Exception as e:
            logger.error(f"获取binlog位置时发生错误: {str(e)}")
            return None, None
    
    except Exception as e:
        logger.error(f"数据初始化过程中发生错误: {str(e)}")
        return None, None
    finally:
        cursor.close()
        conn.close()
        es_client.close()


def main():
    # 解析命令行参数
    parser = argparse.ArgumentParser(description="根据时间范围初始化工单数据到ElasticSearch")
    parser.add_argument("--start", required=True, help="开始时间，格式为 YYYY-MM-DD HH:MM:SS")
    parser.add_argument("--end", help="结束时间，格式为 YYYY-MM-DD HH:MM:SS，默认为当前时间")
    parser.add_argument("--batch", type=int, default=100, help="每批处理的记录数，默认为100")
    
    args = parser.parse_args()
    
    try:
        datetime.datetime.strptime(args.start, "%Y-%m-%d %H:%M:%S")
        if args.end:
            datetime.datetime.strptime(args.end, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        logger.error("时间格式错误，请使用 YYYY-MM-DD HH:MM:SS 格式")
        return
    
    log_file, log_pos = init_data(args.start, args.end, args.batch)
    
    if log_file and log_pos:
        logger.info(f"初始化完成，当前binlog位置: {log_file}:{log_pos}")
        
        try:
            config = configparser.ConfigParser()
            config.read(config_path)
            config.set("binlog", "log_file", log_file)
            config.set("binlog", "log_pos", str(log_pos))
            config.set("binlog", "init_time", "")  # 清空init_time字段
            with open(config_path, 'w') as f:
                config.write(f)
            logger.info("已更新配置文件中的binlog位置并清空init_time")
        except Exception as e:
            logger.error(f"更新配置文件时发生错误: {str(e)}")


if __name__ == "__main__":
    main()