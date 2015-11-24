from six import iteritems

import boto.dynamodb2
from boto.dynamodb2.items import Item as BotoItem
from boto.dynamodb2.table import Table
from boto.dynamodb2.exceptions import ItemNotFound

from hieratic.engine import ItemEngine, CollectionEngine

from hieratic_dynamodb.context import Context


class Item(ItemEngine):

    def __init__(self, collection, raw_item):
        ItemEngine.__init__(self, collection, raw_item)
        self.__item = raw_item

    @property
    def ddb_item(self):
        return self.__item

    def update(self, patch, context, updates):
        if patch:
            for k, v in iteritems(updates):
                self.__item[k] = v
            self.__item.partial_save()
        else:
            if context is None:
                self.__item = BotoItem(self.__item.table, updates)
                self.__item.save(True)
            else:
                context.put_item(self.__table, updates)

    def delete(self, index, context):
        if context is None:
            self.__item.delete()
        else:
            context.delete_item(
                self.__table,
                **(index.make_key_dict_from_dict(self.get_dict()))
            )

    def get_dict(self):
        return self.__item._data


class Collection(CollectionEngine):

    def __init__(self, name, table_name, region, host=None, is_secure=None, port=None):
        kwargs = {}
        if host is not None:
            kwargs['host'] = host
        if is_secure is not None:
            kwargs['is_secure'] = is_secure
        if port is not None:
            kwargs['port'] = port
        self.__table = Table(
            table_name=table_name,
            connection=boto.dynamodb2.connect_to_region(region, **kwargs),
        )

    @property
    def table(self):
        return self.__table

    def create_raw_item(self, index, data_dict, context):
        if context is None:
            self.__table.put_item(data_dict)
        else:
            context.put_item(self.__table, data_dict)
        return BotoItem(self.__table, data_dict, True)

    def retrieve_raw_item(self, key_dict):
        try:
            return self.__table.get_item(**key_dict)
        except ItemNotFound:
            raise KeyError(key_dict)
        except:
            raise

    def query_raw_items(self, index, parent_key_value, **kwargs):
        if parent_key_value is not None:
            kwargs['{}__eq'.format(parent_key_value[0])] = parent_key_value[1]
        return self.__table.query(index=index, **kwargs)

    def bulk_get_raw_items(self, **kwargs):
        return self.__table.batch_get(**kwargs)

    @classmethod
    def get_context(cls, *args, **kwargs):
        return Context()
