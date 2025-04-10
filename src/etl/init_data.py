#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# @author by wangcw @ 2025
# @generate at 2025/4/10
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
from src.event_processor import EventProcessor
from src.main import dict_to_json

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


def init_data(start_time, end_time, batch_size=100):
    """
    根据时间范围初始化工单数据到ElasticSearch
    
    Args:
        start_time: 开始时间，格式为 YYYY-MM-DD HH:MM:SS
        end_time: 结束时间，格式为 YYYY-MM-DD HH:MM:SS
        batch_size: 每批处理的记录数，默认为100
    """
    logger.info(f"开始初始化数据，时间范围: {start_time} 至 {end_time}")
    
    # 连接数据库
    try:
        conn = pymysql.connect(**DB_SETTINGS)
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        logger.info("数据库连接成功")
    except Exception as e:
        logger.error(f"数据库连接失败: {str(e)}")
        return
    
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
        # 查询符合条件的工单总数
        count_sql = """
        SELECT COUNT(*) as total 
        FROM tb_workorderinfo 
        WHERE CreatedAt BETWEEN %s AND %s
        """
        cursor.execute(count_sql, (start_time, end_time))
        total_count = cursor.fetchone()["total"]
        logger.info(f"找到符合条件的工单数: {total_count}")
        
        if total_count == 0:
            logger.warning("没有找到符合条件的工单数据")
            return
        
        # 分批查询工单数据
        offset = 0
        processed_count = 0
        
        while offset < total_count:
            # 查询一批工单数据
            query_sql = """
            SELECT * 
            FROM tb_workorderinfo 
            WHERE CreatedAt BETWEEN %s AND %s
            ORDER BY Id
            LIMIT %s OFFSET %s
            """
            cursor.execute(query_sql, (start_time, end_time, batch_size, offset))
            records = cursor.fetchall()
            
            # 处理每条工单数据
            for record in records:
                # 构造事件数据
                event = {
                    "schema": src_database,
                    "table": "tb_workorderinfo",
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
                    if processed_count % 10 == 0:
                        logger.info(f"已处理 {processed_count}/{total_count} 条记录")
            
            # 更新偏移量
            offset += batch_size
        
        logger.success(f"数据初始化完成，共处理 {processed_count}/{total_count} 条记录")
    
    except Exception as e:
        logger.error(f"数据初始化过程中发生错误: {str(e)}")
    finally:
        # 关闭连接
        cursor.close()
        conn.close()
        es_client.close()


def main():
    # 解析命令行参数
    parser = argparse.ArgumentParser(description="根据时间范围初始化工单数据到ElasticSearch")
    parser.add_argument("--start", required=True, help="开始时间，格式为 YYYY-MM-DD HH:MM:SS")
    parser.add_argument("--end", required=True, help="结束时间，格式为 YYYY-MM-DD HH:MM:SS")
    parser.add_argument("--batch", type=int, default=100, help="每批处理的记录数，默认为100")
    
    args = parser.parse_args()
    
    # 验证时间格式
    try:
        datetime.datetime.strptime(args.start, "%Y-%m-%d %H:%M:%S")
        datetime.datetime.strptime(args.end, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        logger.error("时间格式错误，请使用 YYYY-MM-DD HH:MM:SS 格式")
        return
    
    # 初始化数据
    init_data(args.start, args.end, args.batch)


if __name__ == "__main__":
    main()