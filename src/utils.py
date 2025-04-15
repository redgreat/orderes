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

def process_bu_json_field(value):
    """专门处理Bu*Json字段，处理字节字符串表示法和Unicode编码"""
    if isinstance(value, str) and value.startswith('{"SignInSummary":'):
        return value
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