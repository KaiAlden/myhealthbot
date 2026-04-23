
class PromptModel:
    '''会话的聊天记录模型'''

    def __init__(self, id=None, user_id=None, role=None, content=None, time=None):
        self.id = id
        self.user_id = user_id
        self.role = role 
        self.content = content
        self.time = time

    def to_history(self) -> dict:
        return {
            "id": self.id,
            "role": self.role,
            "content": self.content,
        }

    def to_Prompt(self) -> dict:
        return {
            "role": self.role,
            "content": self.content
        }