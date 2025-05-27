#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# @author by wangcw @ 2025
# @generate at 2025-4-10 15:58:28
# comment: 事件处理器基类

from elasticsearch import Elasticsearch
from loguru import logger
from typing import Dict, Any, Optional, Callable
import os
import configparser
import time

config = configparser.ConfigParser()
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
config_path = os.path.join(project_root, "conf", "db.cnf")
config.read(config_path)
index_name = config.get("target", "index_name")

class BaseProcessor:
    """事件处理器基类，提供基础的ES操作方法"""
    def __init__(self, es_client: Elasticsearch):
        self.es_client = es_client
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
    
    def _execute_es(self, operation: str, doc_id: str, doc_body: Dict) -> bool:
        """执行ES操作"""
        try:
            if operation == "index":
                # 使用upsert操作，如果文档不存在则创建，存在则更新
                self.es_client.update(
                    index=index_name,
                    id=doc_id,
                    body={"doc": doc_body, "doc_as_upsert": True}
                )
                # logger.success(f"ES索引成功: 索引={index_name}, ID={doc_id}")
                return True
            else:
                logger.warning(f"未定义的ES操作: {operation}")
                return False
        except Exception as e:
            logger.error(f"ES操作失败: 索引={index_name}, ID={doc_id}, {str(e)}")
            return False
    
    def _update_with_retry(self, doc_id: str, update_func: Callable, create_doc_func: Callable, max_retries: int = 3, retry_delay: float = 0.5) -> bool:
        """带有重试机制的ES更新操作
        
        Args:
            doc_id: 文档ID
            update_func: 更新函数，接收当前文档和版本号，返回更新后的结果
            create_doc_func: 创建文档函数，当文档不存在时调用
            max_retries: 最大重试次数
            retry_delay: 初始重试延迟（秒）
            
        Returns:
            bool: 操作是否成功
        """
        retries = 0
        while retries <= max_retries:
            try:
                # 先获取当前文档，包括版本号
                try:
                    current_doc = self.es_client.get(index=index_name, id=doc_id)
                    version = current_doc.get('_version')
                    source = current_doc.get('_source', {})
                    # 调用更新函数
                    return update_func(source, version)
                except Exception as e:
                    if "document_missing_exception" in str(e) or "404" in str(e):
                        # 文档不存在，调用创建函数
                        return create_doc_func()
                    else:
                        raise e
                        
            except Exception as e:
                if "version_conflict_engine_exception" in str(e) and retries < max_retries:
                    # 版本冲突，等待后重试
                    retries += 1
                    # 指数退避策略
                    sleep_time = retry_delay * (2 ** (retries - 1))
                    logger.warning(f"ES更新版本冲突，第{retries}次重试: 索引={index_name}, ID={doc_id}, 等待{sleep_time}秒")
                    time.sleep(sleep_time)
                    continue
                elif "document_missing_exception" in str(e) or "404" in str(e):
                    # 文档不存在，调用创建函数
                    return create_doc_func()
                else:
                    logger.error(f"ES更新失败: 索引={index_name}, ID={doc_id}, {str(e)}")
                    return False
        
        logger.error(f"ES更新失败，超过最大重试次数: 索引={index_name}, ID={doc_id}")
        return False