# from typing import Mapping, List

# from simplyblock_core.models.base_model import BaseModel


# class User(BaseModel):
#     attributes = {
#         "uuid":{"type": str, 'default': ""},
#         "name": {"type": str, 'default': ""},
#         "username": {"type": str, 'default': ""},
#         "email": {"type" : str, 'default':""},
#         "secret": {"type": str, "default": ""},
#         "updated_at": {"type": str, "default": ""},
#     }
    

#     def __init__(self, data=None):
#         super(User, self).__init__()
#         self.set_attrs(self.attributes, data)
#         self.object_type = "object"

#     def get_id(self):
#         return self.uuid
           