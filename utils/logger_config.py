''' 日志配置模块：
'   提供日志配置功能，其他模块可以直接使用Python标准库的logging模块
'   使用时： 
'   1. 使用logging前，必须先调用setup_logging()进行初始化
'   1. 导入: import logging
'   3. 使用: logger = logging.getLogger('sedoc.模块名') 创建logger对象
'   Author： yueye 2024-01-10
'''
import os
import logging
from logging import handlers

# 全局变量，记录是否已初始化
_logging_initialized = False

# 返回日志输出的路径
def get_log_path(log_path="logs"):
    """获取日志路径
    Args:
        log_path (str): 相对日志路径或绝对路径
    Returns:
        str: 完整的日志路径
    """
    # 使用os.getcwd()直接获取当前工作目录作为项目根目录
    project_root = os.getcwd()
    
    # 构建完整的日志路径
    if os.path.isabs(log_path):
        logpath = log_path
    else:
        # 移除log_path开头的斜杠，避免路径拼接问题
        if log_path.startswith('/') or log_path.startswith('\\'):
            log_path = log_path[1:]
        # 处理log_path结尾的斜杠
        if log_path.endswith('/') or log_path.endswith('\\'):
            log_path = log_path.rstrip('/\\')
        
        logpath = os.path.join(project_root, log_path)
    
    # 检查文件夹是否存在，不存在则创建
    if not os.path.exists(logpath):
        print(f"日志目录不存在，创建: {logpath}")
        try:
            os.makedirs(logpath)
        except Exception as e:
            print(f"创建日志目录失败: {e}")
    
    return logpath

def setup_logging(name="", level=logging.DEBUG, log_path="logs"):
    """配置全局日志系统
    此函数负责配置全局根日志系统, 全局调用一次即可，后续模块直接调用logging.getLogger()获取logger对象即可。
    通过配置根logger实现全局日志设置，所有子logger都会自动继承这些设置。
    日志文件使用TimedRotatingFileHandler实现按天滚动，自动清理过期日志。
    
    Args:
        name (str): logger名称，默认为空字符串(表示根logger)
        level (int): 日志级别，默认为logging.DEBUG
        log_path (str): 日志文件路径，默认为"logs"
    """
    global _logging_initialized
    if _logging_initialized:
        print("日志系统已经初始化，跳过重复初始化")
        return
    
    try:
        # 获取日志输出的路径
        base_dir = get_log_path(log_path)
        
        # 获取根logger（空字符串表示根logger）
        root_logger = logging.getLogger(name)
        root_logger.setLevel(level)
        
        # 清除已有的handler
        if root_logger.handlers:
            for handler in root_logger.handlers:
                root_logger.removeHandler(handler)
        
        # 自定义日志格式
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s')

        # 实例化控制台渠道
        sh = logging.StreamHandler()
        sh.setLevel(logging.DEBUG)
        sh.setFormatter(formatter)
        root_logger.addHandler(sh)

        # 实例化所有日志文件渠道 - 使用TimedRotatingFileHandler实现定期清理
        infoformatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        all_log_path_file = os.path.join(base_dir, "info.log")
        try:
            # 使用TimedRotatingFileHandler实现按天滚动，保留7天的日志
            # when='midnight' 每天午夜滚动
            # interval=1 每天一个文件
            # backupCount=7 保留7天的日志
            # delay=True 延迟创建文件，直到第一次写入
            fh = logging.handlers.TimedRotatingFileHandler(
                all_log_path_file, 
                when='midnight', 
                interval=1, 
                backupCount=7, 
                encoding='utf-8',
                delay=True
            )
            fh.setLevel(logging.DEBUG)
            fh.setFormatter(infoformatter)
            root_logger.addHandler(fh)
        except Exception as e:
            print(f"配置logging日志，添加INFO文件handler失败: {e}")

        # 实例化错误日志文件渠道 - 使用TimedRotatingFileHandler实现定期清理
        error_log_path_file = os.path.join(base_dir, "error.log")
        try:
            # 使用TimedRotatingFileHandler实现按天滚动，保留14天的错误日志
            fh1 = logging.handlers.TimedRotatingFileHandler(
                error_log_path_file, 
                when='midnight', 
                interval=1, 
                backupCount=14, 
                encoding='utf-8',
                delay=True
            )
            fh1.setLevel(logging.ERROR)
            fh1.setFormatter(formatter)
            root_logger.addHandler(fh1)
        except Exception as e:
            print(f"配置logging日志，添加ERROR文件handler失败: {e}")
        
        # 设置根logger的传播属性为False, 根logger已经是顶层logger，不需要再向上传播
        root_logger.propagate = False
        
        # 标记已初始化
        _logging_initialized = True

        print(f"日志系统初始化成功，日志路径: {base_dir}")
    except Exception as e:
        print(f"日志系统初始化失败: {e}")
        import traceback
        traceback.print_exc()
        # 即使失败也标记为已尝试初始化，避免重复报错
        _logging_initialized = True

def cleanup_logging(name="sedoc"):
    """清理日志处理器，安全关闭所有文件句柄
    Args:
        name (str): logger名称
    """
    logger = logging.getLogger(name)
    # 复制handlers列表，避免在迭代时修改列表
    handlers = list(logger.handlers)
    for handler in handlers:
        try:
            # 先移除handler，再关闭它
            logger.removeHandler(handler)
            handler.close()
        except Exception:
            # 如果关闭handler时出错，不要抛出异常
            pass
