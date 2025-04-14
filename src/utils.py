#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# @author by wangcw @ 2025
# @generate at 2025/4/14 16:34
# comment: 通用工具函数

import datetime
import decimal
import json

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

def dict_to_json(res_value):
    """将字典转换为JSON字符串，处理特殊类型
    
    Args:
        res_value: 要转换的值
        
    Returns:
        str: JSON字符串
    """
    def ensure_serializable(obj):
        """确保对象可以被JSON序列化"""
        if isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, decimal.Decimal):
            return str(obj)
        elif isinstance(obj, bytes):
            try:
                return obj.decode('utf-8')
            except UnicodeDecodeError:
                return obj.hex()
        elif isinstance(obj, (dict, list, str, int, float, bool, type(None))):
            return obj
        else:
            return str(obj)
    
    if isinstance(res_value, dict):
        for key, value in res_value.items():
            res_value[key] = ensure_serializable(value)
        return json.dumps(res_value, ensure_ascii=False)
    else:
        return json.dumps(ensure_serializable(res_value), ensure_ascii=False)

def process_bu_json_field(value):
    """专门处理Bu*Json字段，处理字节字符串表示法和Unicode编码"""
    if isinstance(value, bytes):
        try:
            return value.decode('utf-8')
        except UnicodeDecodeError:
            return value.hex()
    
    if isinstance(value, str):
        value = value.strip("'")
        try:
            # 尝试解析JSON字符串
            if value.startswith('{') and value.endswith('}'):
                # 替换单引号为双引号以符合JSON标准
                return json.loads(value.replace("'", '"'))
            elif value.startswith('[') and value.endswith(']'):
                return json.loads(value.replace("'", '"'))
        except json.JSONDecodeError:
            # 如果不是有效的JSON，则原样返回
            pass
        
        # 处理转义Unicode字符
        try:
            # 检查是否包含\u开头的Unicode编码
            if '\\u' in value:
                return value.encode('utf-8').decode('unicode_escape')
        except UnicodeError:
            pass
        
        return value
    
    return value


def process_extra_json(value):
    """处理ConfigValue等字段中的JSON字符串数据
    
    Args:
        value: 要处理的值，可能是JSON字符串
        
    Returns:
        解析后的对象或原值
    """
    if not isinstance(value, str):
        return value
        
    value = value.strip()
    try:
        if (value.startswith('{') and value.endswith('}')) or (value.startswith('[') and value.endswith(']')):
            # 对单引号进行处理，确保符合JSON语法
            normalized_json = value.replace("'", '"')
            # 对非标准的键值对形式进行处理
            normalized_json = normalized_json.replace(':', ': ')
            return json.loads(normalized_json)
    except json.JSONDecodeError:
        # 如果解析失败，返回原始值
        pass
    
    return value
