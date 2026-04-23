
import json

class ResultModel:
    '''请求的返回信息'''

    def __init__(self, success: bool, message: str = None, data: dict|str = None):
        self.success = success
        self.message = message
        self.data = data

    def to_stream(self) -> str:
        '''返回流式数据'''
        json_data = json.dumps({
            "success": self.success,
            "message": self.message,
            "data": self.data
        }, ensure_ascii=False)
        return f'data: {json_data}\n\n'

    def to_json(self) -> str:
        '''返回字典'''
        return json.dumps({
            "success": self.success,
            "message": self.message,
            "data": self.data
        }, ensure_ascii=False)

    @staticmethod   
    def success(data: dict|str = None):
        '''成功返回'''
        return ResultModel(True, "请求成功", data)

    @staticmethod   
    def failure(message: str):
        '''失败返回'''
        return ResultModel(False, message)