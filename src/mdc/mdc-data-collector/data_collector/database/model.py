import enum
import datetime
from sqlalchemy import Column, Integer, String, Unicode, DateTime, Float
from restful_modbus_api.database import Base

__all__ = ('FiledType', 'create_members', 'get_model')


class FiledType(enum.Enum):
    INTEGER = Integer
    FLOAT = Float
    UNICODE = Unicode
    DATETIME = DateTime


def as_dict(self):
    data = dict()
    for c in self.__table__.columns:
        value = getattr(self, c.name)
        if isinstance(value, datetime.datetime):
            value = value.strftime('%Y-%m-%d %H:%M:%S')
        data[c.name] = value
    return data


def repr_function(self):
    members = [f'{f}={getattr(self, f)}' for f in self.__fields__]
    return f"<{self.__class__.__name__}({', '.join(members)})>"


def init(self, *args, **kwargs):
    for arg in zip(self.__fields__, args):
        setattr(self, arg[0], arg[1])


def create_members(columns: list):
    """
    Column 에 대한 정보
    :param columns:
    :param fields:
    :return:
    """
    fields = [
        {'field_name': 'id',
         'type': FiledType.INTEGER,
         'options': {'primary_key': True, 'autoincrement': True},
         'is_field': False},
        {'field_name': 'timestamp',
         'type': FiledType.DATETIME,
         'options': {'nullable': True},
         'is_field': True}, ]
    fields += columns

    members = dict()

    for field in fields:

        members[field['field_name']] = Column(
            field['type'].value, **field['options'])

    members['__init__'] = init
    members['__fields__'] = [f['field_name'] for f in fields if f['is_field']]
    # member functions
    members['as_dict'] = as_dict
    members['__repr__'] = repr_function
    return members


def get_model(class_name, member):
    member = create_members(member)
    member['__tablename__'] = class_name
    c = type(class_name, (Base,), member)
    return c
