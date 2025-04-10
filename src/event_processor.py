#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# @author by wangcw @ 2025
# @generate at 2025/3/24 09:33
# comment: 事件处理器

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError
from loguru import logger
import json
from typing import Dict, Any, Optional
import importlib

# 从基类导入索引名称
from src.base_processor import BaseProcessor, index_name

class EventProcessor(BaseProcessor):
    """事件处理器基类，接收JSON数据并根据表名分发到不同的处理方法"""
    def __init__(self, es_client):
        super().__init__(es_client)
        self.handlers = {}
        self._init_handlers()

    def _init_handlers(self):
        """延迟导入处理器类，避免循环导入问题"""
        # 动态导入处理器类
        from src.handlers._order_handler import OrderHandler
        from src.handlers._status_handler import StatusHandler
        from src.handlers._car_handler import CarHandler
        from src.handlers._service_handler import ServiceHandler
        from src.handlers._record_handler import RecordHandler
        from src.handlers._appointment_handler import AppointmentHandler
        from src.handlers._appointment_concat_handler import AppointmentConcatHandler
        from src.handlers._operating_handler import OperatingHandler
        from src.handlers._json_handler import JsonHandler
        from src.handlers._column_handler import ColumnHandler
        from src.handlers._config_handler import ConfigHandler
        from src.handlers._signin_handler import SigninHandler
        
        # 初始化处理器映射
        self.handlers = {
            "tb_workorderinfo": OrderHandler(self.es_client),
            "tb_workorderstatus": StatusHandler(self.es_client),
            "tb_workcarinfo": CarHandler(self.es_client),
            "tb_workserviceinfo": ServiceHandler(self.es_client),
            "tb_recordinfo": RecordHandler(self.es_client),
            "tb_appointment": AppointmentHandler(self.es_client),
            "tb_appointmentconcat": AppointmentConcatHandler(self.es_client),
            "tb_operatinginfo": OperatingHandler(self.es_client),
            "tb_workbussinessjsoninfo": JsonHandler(self.es_client),
            "tb_custcolumn": ColumnHandler(self.es_client),
            "basic_custspecialconfig": ConfigHandler(self.es_client),
            "tb_worksignininfo": SigninHandler(self.es_client)
        }

    def handle_event(self, action: str, data: Dict) -> bool:
        """统一事件处理入口，根据表名分发到不同的处理方法
        Args:
            action: 操作类型 (insert, update, delete)
            data: 事件数据
        Returns:
            bool: 处理是否成功
        """
        table = data.get('table')
        if table in self.handlers:
            return self.handlers[table].handle(action, data)
        else:
            logger.warning(f"未找到表 {table} 的处理器")
            return False
 