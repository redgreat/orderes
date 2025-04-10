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
    level="INFO",
)

# 数据库连接设置
DB_SETTINGS = {
    "host": src_host,
    "port": src_port,
    "user": src_user,
    "password": src_password,
    "database": src_database,
    "charset": src_charset,
}

# ElasticSearch连接配置
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
        sql_query: SQL查询语句，必须包含 {id_placeholder} 占位符
        order_ids: 工单ID列表
        batch_size: 每批处理的记录数，默认为100
        
    Returns:
        int: 成功处理的记录数
    """
    processed_count = 0
    total_ids = len(order_ids)
    logger.info(f"开始处理表 {table_name}，共有 {total_ids} 个工单ID")
    
    # 分批处理工单ID
    for i in range(0, total_ids, batch_size):
        # 获取当前批次的ID列表
        batch_ids = order_ids[i:i+batch_size]
        
        # 构建IN查询的参数字符串
        id_placeholder = ", ".join(["%s"] * len(batch_ids))
        
        # 替换SQL查询中的占位符
        current_sql = sql_query.format(id_placeholder=id_placeholder)
        
        try:
            # 执行查询
            cursor.execute(current_sql, batch_ids)
            records = cursor.fetchall()
            
            # 处理每条记录
            for record in records:
                # 构造事件数据
                event = {
                    "schema": src_database,
                    "table": table_name,
                    "action": "update"
                }
                event.update(record)
                
                # 转换为JSON数据
                json_data = json.loads(dict_to_json(event))
                
                # 使用处理器处理事件
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
    # 如果没有提供结束时间，使用当前时间
    if end_time is None:
        end_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
    logger.info(f"开始初始化数据，时间范围: {start_time} 至 {end_time}")
    
    # 连接数据库
    try:
        conn = pymysql.connect(**DB_SETTINGS)
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        logger.info("数据库连接成功")
    except Exception as e:
        logger.error(f"数据库连接失败: {str(e)}")
        return None, None
    
    # 创建ElasticSearch连接
    try:
        es_client = Elasticsearch(**ES_SETTINGS)
        logger.info("ElasticSearch连接成功")
    except Exception as e:
        logger.error(f"ElasticSearch连接失败: {str(e)}")
        cursor.close()
        conn.close()
        return
    
    # 创建事件处理器
    processor = EventProcessor(es_client)
    
    try:
        # 查询符合条件的工单ID列表
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
        
        # 提取工单ID列表
        order_ids = [str(record['Id']) for record in id_records]
        total_count = len(order_ids)
        logger.info(f"找到符合条件的工单数: {total_count}")
        
        # 定义需要处理的表和对应的SQL查询
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
            WHERE WorkOrderId IN ({id_placeholder})
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
        
        # 处理每个表的数据
        total_processed = 0
        for table_name, sql_query in tables_to_process.items():
            processed = process_table(
                conn, cursor, processor, 
                table_name, sql_query, 
                order_ids, batch_size
            )
            total_processed += processed
        
        logger.success(f"数据初始化完成，共处理 {total_processed} 条记录")
        
        # 获取当前binlog位置
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
        # 关闭连接
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
    
    # 验证时间格式
    try:
        datetime.datetime.strptime(args.start, "%Y-%m-%d %H:%M:%S")
        if args.end:
            datetime.datetime.strptime(args.end, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        logger.error("时间格式错误，请使用 YYYY-MM-DD HH:MM:SS 格式")
        return
    
    # 初始化数据
    log_file, log_pos = init_data(args.start, args.end, args.batch)
    
    if log_file and log_pos:
        logger.info(f"初始化完成，当前binlog位置: {log_file}:{log_pos}")
        
        # 更新配置文件中的binlog位置
        try:
            config = configparser.ConfigParser()
            config.read(config_path)
            config.set("binlog", "log_file", log_file)
            config.set("binlog", "log_pos", str(log_pos))
            with open(config_path, 'w') as f:
                config.write(f)
            logger.info("已更新配置文件中的binlog位置")
        except Exception as e:
            logger.error(f"更新配置文件时发生错误: {str(e)}")


if __name__ == "__main__":
    main()