#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# @author by wangcw @ 2025
# comment: 车辆信息处理器

from elasticsearch import Elasticsearch
from loguru import logger
from typing import Dict, Any
from ..event_processor import EventProcessor, index_name

class CarHandler(EventProcessor):
    """处理tb_workcarinfo表的事件，存入CarInfo嵌套字段"""
    def handle(self, action: str, data: Dict) -> bool:
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