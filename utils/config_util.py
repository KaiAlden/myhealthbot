import json
"""加载配置文件"""

def get_config(path, node=None) -> dict:
    try:
        with open(path, "r", encoding="utf-8-sig") as config_file:
            config = json.load(config_file)

        if node is None:
            return config
        return config[node]
    except FileNotFoundError:
        print("错误：未找到配置文件，请检查路径是否正确。")
        return None
    except json.JSONDecodeError as exc:
        print(f"错误：配置文件格式不正确，请检查 JSON 格式是否有效。详细信息：{exc}")
        return None
    except KeyError as exc:
        print(f"错误：配置项缺失：{exc}")
        return None
    except Exception as exc:
        print(f"错误：读取配置文件失败：{exc}")
        return None
