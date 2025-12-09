import json
import re
import lark_oapi as lark
import pandas as pd
from lark_oapi.api.sheets.v3 import *
import requests
from requests_toolbelt import MultipartEncoder
from tqdm import tqdm
from fastapi import FastAPI, HTTPException, Query
import time

class goodlark:
    """
    v 1.1
    Wish you GOOD LARK!

    @ Wu, J. J.
    with lark_oapi

    一项用于本地调用飞书表格的类，内部提供了一些常用的飞书表格操作方法（可根据需要更新）。
    目前提供的方法包括：
    1. 功能性：
    get_tenant_access_token: 获取tenant_access_token
    generate_col_order_info: 得到当前预定义'列索引→排序'、'排序→列索引'的映射字典（A到ZZ）
    get_name_id_dict: 获取指定电子表格下所有sheet的名称与id的字典
    to_2d_list: 将dataframe与dict转为标准二维列表，用于后续数据处理与写入飞书表格
    complete_data_range: 基于二维列表形状，将起始数据范围补全为完整数据范围

    2. 创建电子表格与sheet:
    create_spreadsheet: 在指定飞书文件夹下创建一个电子表格（请确保bot有该文件夹权限。可将bot加入到群组，再将文件夹共享给群组）
    create_sheet: 在指定电子表格下创建一个sheet（请确保bot有该电子表格权限）

    3. 读取飞书表格：
    load_data_from_sheet: 从指定电子表格下的sheet中加载指定范围数据（请确保bot有该电子表格权限）
    load_sheet_data_to_df: 将指定电子表格下的sheet数据加载到pandas DataFrame中（请确保bot有该电子表格权限）（可分批读取）
    select_from_sheet_to_df: 为load_sheet_data_to_df设定一部分参数，没有设定的参数可以让使用者灵活输入
    
    4. 写入飞书表格：    
    write_single_data: 在指定电子表格、指定sheet、指定单个单元格写入数据（请确保bot有电子表格权限）
    write_multi_data: 在指定电子表格、指定sheet、指定起始单元格写入范围数据（请确保bot有电子表格权限）
    batch_write_multi_data: 在指定电子表格、指定sheet、指定起始单元格 批量 写入范围数据（请确保bot有电子表格权限）
    select_batch_write_multi_data: 为batch_write_multi_data设定一部分参数，没有设定的参数可以让使用者灵活输入

    v 1.0修改：
    1. write_single_data / write_multi_data / batch_write_multi_data: 处理了np.nan等空值无法写入的问题
    v 1.1修改：
    1. 移除了属性self.bearer_token，改为在需要时调用get_tenant_access_token方法获取，防止过期问题（tenant_access_token最大有效期是两小时）
    2. load_data_from_sheet / load_sheet_data_to_df / write_multi_data / batch_write_multi_data: 均改为及时刷新bearer_token
    """

    def __init__(self, app_id: str, app_secret: str):
        """
        初始化goodlark，需要输入飞书应用的app_id与app_secret。
        app_id: 飞书应用的app_id
        app_secret: 飞书应用的app_secret
        
        类属性:
        app_id: 飞书应用的app_id
        app_secret: 飞书应用的app_secret
        client: 飞书客户端对象
        col_order_info: 包含当前预定义'列索引→排序'、'排序→列索引'的映射字典（A到ZZ）
        """
        self.app_id = app_id
        self.app_secret = app_secret
        self.client = lark.Client.builder() \
            .app_id(self.app_id) \
            .app_secret(self.app_secret) \
            .log_level(lark.LogLevel.INFO) \
            .build()
        self.col_order_info = self.generate_col_order_info()

    def get_tenant_access_token(self):
        """
        获取tenant_access_token
        return: tenant_access_token
        """

        url = 'https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal'
        try:
            response = requests.post(url, json={"app_id": self.app_id, "app_secret": self.app_secret})
            response.raise_for_status()
            tenant_access_token = response.json()['tenant_access_token']
            return tenant_access_token
        except Exception as e:
            raise Exception(f"Failed: get_tenant_access_token. {e}")
    
    def generate_col_order_info(self):
        """
        生成A到ZZ的完整列表。得到当前预定义'列索引→排序'、'排序→列索引'的映射字典
        return: 一个包含col_order_dict（列索引映射排序）与order_col_dict（排序映射列索引）的字典
        """
        A_Z_list = [chr(ord('A') + i) for i in range(26)]
        AA_ZZ_list = [f"{chr(ord('A') + i)}{chr(ord('A') + j)}" for i in range(26) for j in range(26)]
        A_ZZ_list = A_Z_list + AA_ZZ_list

        col_to_order = {col: i for i, col in enumerate(A_ZZ_list)}
        order_to_col = {i:col for i, col in enumerate(A_ZZ_list)}
        col_order_info = {'col_to_order': col_to_order, 'order_to_col': order_to_col}

        return col_order_info

    
    def create_spreadsheet(self, folder_token: str, spreadsheet_name: str):
        """
        在指定飞书文件夹下创建一个电子表格（请确保bot有该文件夹权限。可将bot加入到群组，再将文件夹分享给群组）
        folder_token: 飞书文件夹的token
        spreadsheet_name: 想给电子表格取的名称
        return: 一个包含电子表格url与spreadsheet_token的字典
        """

        # 构造请求对象
        request: CreateSpreadsheetRequest = CreateSpreadsheetRequest.builder() \
            .request_body(Spreadsheet.builder()
                .title(spreadsheet_name)
                .folder_token(folder_token)
                .build()) \
            .build()
        
        try:
            # 发起请求
            response: CreateSpreadsheetResponse = self.client.sheets.v3.spreadsheet.create(request)
            if not response.success():
                raise Exception(f"请求失败。{response.code}。{response.msg}")
            
            url = response.data.spreadsheet.url
            spreadsheet_token = response.data.spreadsheet.spreadsheet_token
            spreadsheet_info_dict = {'url': url, 'spreadsheet_token': spreadsheet_token}
            
            print(f"成功在'{folder_token}'创建电子表格'{spreadsheet_name}'。url: {url}; spreadsheet_token: {spreadsheet_token}")
            return spreadsheet_info_dict
        
        except Exception as e:
            raise Exception(f"Failed: create_spreadsheet. {e}")

    def create_sheet(self, spreadsheet_token: str, sheet_name: str):
        """
        在指定电子表格中创建一个sheet（请确保bot有该电子表格权限）
        spreadsheet_token: 电子表格的token
        sheet_name: 想给sheet取的名称
        return: 一个包含sheet_name与sheet_id的字典
        """

        # 构造请求对象
        body = {"index": 0, "title": sheet_name}
        request: lark.BaseRequest = lark.BaseRequest.builder() \
            .http_method(lark.HttpMethod.POST) \
            .uri(f"/open-apis/sheets/v3/spreadsheets/{spreadsheet_token}/sheets") \
            .token_types({lark.AccessTokenType.TENANT}) \
            .body(body) \
            .build()
        
        try:
            # 发起请求
            response: lark.BaseResponse = self.client.request(request)
            if not response.success():
                raise Exception(f"请求失败。{response.code}。{response.msg}")

            # 因BaseResponse不包含Data（title、sheet_id）信息，因此调用get_name_id_dict方法得到id信息
            sheet_id = self.get_name_id_dict(spreadsheet_token)[sheet_name]
            # 构建sheet_info
            sheet_info_dict = {'sheet_name': sheet_name, 'sheet_id': sheet_id}

            print(f"成功在'{spreadsheet_token}'创建'{sheet_name}'。sheet_id: {sheet_id}")
            return sheet_info_dict

        except Exception as e:
            raise Exception(f"Failed: create_sheet. {e}")
            

    def get_name_id_dict(self, spreadsheet_token: str):
        """
        获取一个电子表格中所有sheet的”名称“与”sheet_id“映射
        """

        # 构造请求对象
        request: QuerySpreadsheetSheetRequest = QuerySpreadsheetSheetRequest.builder() \
            .spreadsheet_token(spreadsheet_token) \
            .build()

        try:
            # 发送请求
            response: QuerySpreadsheetSheetResponse = self.client.sheets.v3.spreadsheet_sheet.query(request)
            if not response.success():
                raise Exception(f"请求失败。{response.code}。{response.msg}")
            # 正常处理返回映射dict
            name_id_dict = {}
            sheet_infos = json.loads(lark.JSON.marshal(response.data.sheets))
            for sheet_info in sheet_infos:
                name_id_dict[sheet_info['title']] = sheet_info['sheet_id']
            return name_id_dict
        # 无法得到映射dict则报错
        except Exception as e:
            raise Exception(f"Failed: get_name_id_dict. {e}")

    def load_data_from_sheet(self, spreadsheet_token: str, sheet_name: str, data_range: str):
        """
        读取指定表格，指定sheet名称，指定范围的全部单元格数据
        spreadsheet_token: 电子表格的token
        sheet_name: sheet的名称
        data_range: 要读取数据的范围。如"A1:K10"
        return: 包含指定范围单元格数据的列表
        """

        # 获取sheet_id（构建url）
        sheet_id = self.get_name_id_dict(spreadsheet_token)[sheet_name]

        # 获取数据位置（构建url）
        data_loc = f"{sheet_id}!{data_range}"
        url = f'https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/values/{data_loc}?valueRenderOption=ToString&dateTimeRenderOption=FormattedString'
        
        # 构建headers（请求头）
        bearer_token = self.get_tenant_access_token()
        headers = {'Authorization': f'Bearer {bearer_token}'}
        
        try:
            response = requests.get(url, headers = headers)
            response.raise_for_status()
            data_loaded = response.json()['data']['valueRange']['values']
            return data_loaded
        except Exception as e:
            raise Exception(f"Failed: load_data_from_sheet. {e}")

    def load_sheet_data_to_df(self, spreadsheet_token: str, sheet_name: str, row_id_start: int, row_id_end: int, col_id_start: str, col_id_end: str, batch_size: int = 100, target_column_names: list = []):
        """
        读取指定表格，指定sheet名称，指定范围的全部单元格数据。筛选整理数据后直接返回dataframe
        spreadsheet_token: 电子表格的token
        sheet_name: sheet的名称
        row_id_start: 飞书表格从第几行开始读取
        row_id_end: 飞书表格读取到第几行结束
        col_id_start: 飞书表格读取的最左列id
        col_id_end: 飞书表格读取的最右列id
        batch_size: 批次读取数据时，每次读取的行数（默认为100）
        target_column_names: 在读取飞书表格后，只会把里面的列读取出来（如果为空，返回所有列）
        return: 一个包含指定范围单元格数据的dataframe
        """

        # 构建headers（请求头）
        bearer_token = self.get_tenant_access_token()
        headers = {'Authorization': f'Bearer {bearer_token}'}

        # 构建url（多轮批次url）
        url_list = []
        ## 获取sheet_id
        sheet_id = self.get_name_id_dict(spreadsheet_token)[sheet_name]
        ## 构建批次读取数据范围，并构建出url
        for current_start_id in range(row_id_start, row_id_end + 1, batch_size):
            current_end_id = min(current_start_id + batch_size - 1, row_id_end)
            data_range = f"{col_id_start}{current_start_id}:{col_id_end}{current_end_id}"
            data_loc = f"{sheet_id}!{data_range}"
            range_url = f'https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/values/{data_loc}?valueRenderOption=ToString&dateTimeRenderOption=FormattedString'
            url_list.append(range_url)
        
        # 读取并整合数据
        try:
            data_all = []
            # 如果并未从第一行开始读取，则手动往数据开头添加表头
            if row_id_start > 1:
                head_section_url = f'https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/values/{sheet_id}!{col_id_start}1:{col_id_end}1?valueRenderOption=ToString&dateTimeRenderOption=FormattedString'
                head_response = requests.get(head_section_url, headers = headers)
                head_response.raise_for_status()
                head_collected = head_response.json()['data']['valueRange']['values']
                data_all.extend(head_collected)

            # 读取范围内数据
            for url in tqdm(url_list, total=len(url_list), desc=f"Loading data from {spreadsheet_token}的{sheet_name}..."):
                ## 每个url最多重试5次
                max_retries = 5
                for attempt in range(max_retries):
                    try:
                        response = requests.get(url, headers = headers)
                        response.raise_for_status()
                        # 读取到二维表数据
                        data_collected = response.json()['data']['valueRange']['values']
                        data_all.extend(data_collected)
                        break
                    except Exception as e:
                        if attempt < (max_retries - 1):
                            wait_time = 10 * (attempt + 1)
                            time.sleep(wait_time)
                        else:
                            raise e
            
            # 整合为dataframe处理
            colname_list = data_all[0]
            data_values = data_all[1:]
            mydf = pd.DataFrame(data_values, columns = colname_list)
            # 删除完全空行，无效空列
            mydf_row_cleaned = mydf.dropna(axis = 0, how = 'all')
            to_drop = [col for col in mydf_row_cleaned.columns if col is None or col == '' or pd.isna(col)]
            mydf_cleaned = mydf_row_cleaned.drop(columns = to_drop)
            # 筛选范围
            if target_column_names:
                mydf_cleaned = mydf_cleaned[target_column_names]
            return mydf_cleaned

        except Exception as e:
            raise Exception(f"Failed: load_sheet_data_to_df. {e}")

    def select_from_sheet_to_df(self, spreadsheet_token: str = None, sheet_name: str = None, row_id_start: int = None, row_id_end: int = None, col_id_start: str = None, col_id_end: str = None, batch_size: int = None, target_column_names: list = []):
        """
        可为load_sheet_data_to_df设定一部分参数，没有设定的参数可以让使用者灵活输入
        return: 一个包含指定范围单元格数据的dataframe
        """

        if not spreadsheet_token:
            spreadsheet_token = input("请输入电子表格的token：")
        if not sheet_name:
            sheet_name = input("请输入表格中要读取sheet的名称：")
        if not row_id_start:
            row_id_start = int(input("请输入要读取的起始'行'："))
        if not row_id_end:
            row_id_end = int(input("请输入读取到哪一'行'结束："))
        if not col_id_start:
            col_id_start = input("请输入要读取的起始'列'：").upper()
        if not col_id_end:
            col_id_end = input("请输入读取到哪一'列'结束：").upper()
        if not batch_size:
            batch_size = int(input("请输入每批读取多少行（建议50-200行）："))
        
        df_loaded = self.load_sheet_data_to_df(spreadsheet_token, sheet_name, row_id_start, row_id_end, col_id_start, col_id_end, batch_size, target_column_names)
        return df_loaded
    
    def to_2d_list(self, input_data):
        """
        将输入数据转换为二维列表
        input_data: 输入数据，可接受dataframe、dict
        return: 转换后的二维列表
        """
        
        # 格式转换得到list_data
        try:
            # dataframe (可自动避免长度不一致)
            if isinstance(input_data, pd.DataFrame):
                list_data = []
                # 取列名
                col_list = input_data.columns.tolist()
                list_data.append(col_list)
                # 取值
                value_list = input_data.values.tolist()
                list_data.extend(value_list)
            
            # dict（需自行判断长度不一致）
            elif isinstance(input_data, dict):
                list_data = []
                #取列名
                key_list = list(input_data.keys())
                list_data.append(key_list)
                # 取值
                value_list = list(input_data.values())
                ## 值长度相同的话，值的长度即样本数量
                value_lengths = [len(val) for val in value_list]
                if len(set(value_lengths)) != 1:
                    raise ValueError("dict中所有值长度必须相同")
                else:
                    sample_num = value_lengths[0]
                    for i in range(sample_num):
                        row_list = [val[i] for val in value_list]
                        list_data.append(row_list)
            else:
                raise ValueError("输入数据类型必须为dataframe、dict")
            
            return list_data
            
        except Exception as e:
            raise Exception(f"Failed: to_2d_list. {e}")
    

    def complete_data_range(self, start_loc: str, input_data: list[list]):
        """
        根据已有的二维列表，补充完整数据范围
        start_loc: 起始位置，例如"A1"
        input_data: 已有的二维列表

        返回：完整的data_range，例如"A1:C10"
        """

        try:
            # 解析起始位置
            search_pattern = r'''([A-Za-z]+)(\d+)'''
            search_match = re.search(search_pattern, start_loc)
            start_letter = search_match.group(1).upper()
            start_number = int(search_match.group(2))
            # 计算结束位置
            ## 判断行数据非空（不能为空[]），并确保数据等长
            elem_len = []
            for elem in input_data:
                if not elem:
                    raise ValueError("行数据不能为空。请使用''、None、Nan等表示空值。")
                else:
                    elem_len.append(len(elem))
            if len(set(elem_len)) != 1:
                raise ValueError("所有元素长度必须相同")
            ## 若为有效数据，则计算结束位置
            else:
                row_num = len(input_data)
                col_num = len(input_data[0])
                end_letter_order = self.col_order_info['col_to_order'][start_letter] + col_num - 1
                end_letter = self.col_order_info['order_to_col'][end_letter_order]
                end_number = start_number + row_num - 1
                data_range = f"{start_letter}{start_number}:{end_letter}{end_number}"

                return data_range
        
        except Exception as e:
            raise Exception(f"Failed: complete_data_range. {e}")

    def write_single_data(self, spreadsheet_token: str, sheet_name: str, start_loc: str, input_data):
        """
        在指定表格、指定sheet、指定单元格写入元素
        spreadsheet_token: 电子表格的token
        sheet_name: 表格中要写入sheet的名称
        start_loc: 要写入的单元格位置，例如"A1"
        input_data: 要写入的元素，当前支持 str、int、float、（None、Nan）
        """

        try:
            sheet_id = self.get_name_id_dict(spreadsheet_token)[sheet_name]
            data_range = f"{start_loc}:{start_loc}"
            # 根据数据类型创建body的values
            # 待post数据为三维list
            post_data = []
            # 每个单元格为二维list
            # 判断None、Nan
            if pd.isna(input_data):
                post_value = [[{'text': {'text': ''}, 'type': 'text'}]]
                post_data.append(post_value)
            # 判断字符串
            elif isinstance(input_data, str):
                post_value = [[{'text': {'text': input_data}, 'type': 'text'}]]
                post_data.append(post_value)
            # 判断数值
            elif isinstance(input_data, (int, float)):
                post_value = [[{'type': 'value', 'value': {'value': str(input_data)}}]]
                post_data.append(post_value)
            else:
                raise ValueError("目前写入仅支持str、int、float、None、Nan")
            
            # 构建请求体body
            body = {'value_ranges': [{'range': f"{sheet_id}!{data_range}", 'values': post_data}]}
            request: lark.BaseRequest = lark.BaseRequest.builder() \
                .http_method(lark.HttpMethod.POST) \
                .uri(f"/open-apis/sheets/v3/spreadsheets/{spreadsheet_token}/sheets/{sheet_id}/values/batch_update?user_id_type=open_id") \
                .token_types({lark.AccessTokenType.TENANT}) \
                .body(body) \
                .build()
            
            # 发送请求
            response: lark.BaseResponse = self.client.request(request)
            # 处理失败
            if not response.success():
                raise Exception(f"请求失败。{response.code}。{response.msg}")
        
        except Exception as e:
            raise Exception(f"Failed: write_single_data. {e}")
    
    def write_multi_data(self, spreadsheet_token: str, sheet_name: str, start_loc: str, input_data: list[list] | pd.DataFrame | dict):
        """
        从指定表格、指定sheet、指定单元格开始写入多行数据
        spreadsheet_token: 电子表格的token
        sheet_name: 表格中要写入sheet的名称
        start_loc: 多行数据的起始单元格位置，例如"A1"
        input_data: 要写入的多行数据，当前支持 list[list]、pd.DataFrame、Dict
        """

        try:
            # 处理传入的不同格式数据
            if isinstance(input_data, (pd.DataFrame, dict)):
                formatted_data = self.to_2d_list(input_data)
            else:
                formatted_data = input_data
            
            # 检查2d_list格式
            ## 判断行数据非空（不能为空[]），并确保数据等长
            elem_len = []
            for elem in formatted_data:
                if not elem:
                    raise ValueError("行数据不能为空。请使用''、None、Nan等表示空值。")
                else:
                    elem_len.append(len(elem))
            if len(set(elem_len)) != 1:
                raise ValueError("所有元素长度必须相同")
            
            # 2d_list格式正确后，处理空值（Nan/None）以符合传输要求
            standard_data = [["" if pd.isna(item) else item for item in sublist]for sublist in formatted_data]

            # 继续后续处理
            # 构建url
            url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/values"
            # 构建请求头headers
            bearer_token = self.get_tenant_access_token()
            headers = {
                'Authorization': f'Bearer {bearer_token}',
                'Content-Type': 'application/json'
            }
            # 构建请求体
            sheet_id = self.get_name_id_dict(spreadsheet_token)[sheet_name]
            data_range = self.complete_data_range(start_loc, standard_data)
            payload = {"valueRange": {"range": f"{sheet_id}!{data_range}", "values": standard_data}}

            # 发送请求
            response = requests.put(url, headers = headers, json = payload)

            # 处理异常情况
            response_dict = response.json()
            if response_dict['code'] != 0:
                if response_dict['code'] == 90227:
                    raise Exception(f"请求失败。单次数据量过大。建议减少数据量或使用batch_write_multi_data方法批量写入。")
                else:
                    raise Exception(f"请求失败。{response_dict['code']}。{response_dict['msg']}")
        
        except Exception as e:
            raise Exception(f"Failed: write_multi_data. {e}")
    
    def batch_write_multi_data(self, spreadsheet_token: str, sheet_name: str, start_loc: str, input_data: list[list] | pd.DataFrame | dict, batch_size: int = 100):
        """
        从指定表格、指定sheet、指定单元格开始写入多行数据，支持批量写入
        spreadsheet_token: 电子表格的token
        sheet_name: 表格中要写入sheet的名称
        start_loc: 多行数据的起始单元格位置，例如"A1"
        input_data: 要写入的多行数据，当前支持 list[list]、pd.DataFrame、Dict
        batch_size: 每次写入的行数，默认100行
        """

        try:
            # 处理传入的不同格式数据
            if isinstance(input_data, (pd.DataFrame, dict)):
                formatted_data = self.to_2d_list(input_data)
            else:
                formatted_data = input_data
            
            # 检查2d_list格式
            ## 判断行数据非空（不能为空[]），并确保数据等长
            elem_len = []
            for elem in formatted_data:
                if not elem:
                    raise ValueError("行数据不能为空。请使用''、None、Nan等表示空值。")
                else:
                    elem_len.append(len(elem))
            if len(set(elem_len)) != 1:
                raise ValueError("所有元素长度必须相同")
            
            # 2d_list格式正确后，处理空值（Nan/None）以符合传输要求
            standard_data = [["" if pd.isna(item) else item for item in sublist]for sublist in formatted_data]

            # 继续后续处理
            # 构建url
            url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/values"
            # 构建请求头headers
            bearer_token = self.get_tenant_access_token()
            headers = {
                'Authorization': f'Bearer {bearer_token}',
                'Content-Type': 'application/json'
            }
            # sheet_id用于构建请求体
            sheet_id = self.get_name_id_dict(spreadsheet_token)[sheet_name]

            # 解析起始行、列
            search_pattern = r'''([A-Za-z]+)(\d+)'''
            search_match = re.search(search_pattern, start_loc)
            start_letter = search_match.group(1).upper()
            start_number = int(search_match.group(2))

            # 分批处理
            total_rows = len(standard_data)
            batch_num = (total_rows // batch_size) + (1 if total_rows % batch_size != 0 else 0)
            for current_start_idx in tqdm(range(0, total_rows, batch_size), total = batch_num, desc = f"Saving data to {spreadsheet_token} 的 {sheet_name}"):
                current_end_idx = min(current_start_idx + batch_size, total_rows)
                # 得到当前批次数据切片
                current_data = standard_data[current_start_idx : current_end_idx]
                # 得到当前批次数据范围
                current_data_range = self.complete_data_range(f"{start_letter}{start_number + current_start_idx}", current_data)
                # 构建当前批次请求体
                current_payload = {"valueRange": {"range": f"{sheet_id}!{current_data_range}", "values": current_data}}
                
                # 每批数据最多尝试5次
                max_attempts = 5
                for attempt in range(max_attempts):
                    # 发送当前批次请求
                    response = requests.put(url, headers = headers, json = current_payload)
                    # 判断并处理异常情况
                    response_dict = response.json()
                    # 如果请求成功则跳出循环，处理下一批数据
                    if response_dict['code'] == 0:
                        break
                    # 如果请求失败，根据错误码处理
                    # 如果错误为90227，说明单次数据量过大，建议减少batch_size
                    elif response_dict['code'] == 90227:
                        raise Exception(f"请求失败。单次数据量过大，建议减少batch_size。当前为{batch_size}行。")
                    # 其他情况的话
                    else:
                        if attempt < (max_attempts - 1):
                            wait_time = 10 * (attempt + 1)
                            time.sleep(wait_time)
                        else:
                            raise Exception(f"请求失败。{response_dict['code']}。{response_dict['msg']}")

        except Exception as e:
            raise Exception(f"Failed: batch_write_multi_data. {e}")
        
    def select_batch_write_multi_data(self, input_data: list[list] | pd.DataFrame | dict, spreadsheet_token: str = None, sheet_name: str = None, start_loc: str = None, batch_size: int = None):
        """
        可为batch_write_multi_data设定一部分参数，没有设定的参数可以让使用者灵活输入
        """

        if not spreadsheet_token:
            spreadsheet_token = input("请输入电子表格的token：")
        if not sheet_name:
            sheet_name = input("请输入要写入sheet的名称：")
        if not start_loc:
            start_loc = input("请输入要写入的起始单元格（如A1）：")
        if not batch_size:
            batch_size = int(input("请输入每批写入多少行（建议50-200行）："))
        
        self.batch_write_multi_data(spreadsheet_token, sheet_name, start_loc, input_data)










