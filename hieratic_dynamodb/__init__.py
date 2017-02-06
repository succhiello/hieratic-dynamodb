from six import iteritems

from collections import MutableMapping

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

from hieratic.engine import ItemEngine, CollectionEngine

from hieratic_dynamodb.context import Context


class Item(ItemEngine):

    def __init__(self, collection, raw_item):
        ItemEngine.__init__(self, collection, raw_item)
        self.__item = raw_item

    def update(self, patch, context, updates):
        if patch:
            self.collection.table.update_item()
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

    def __make_update_expression_args(self, updates):
        flattened = self.__flatten_dict(updates)
        return {
            UpdateExpression: 'set {0}'.format(['{0}={1}'.format(x[0], x[1]) for x in flattened].join(', ')),
            ExpressionAttributeValues=dict([(x[1], x[2]) for x in flattened])
        }
    
    def __flatten_dict(self, v, keys, acc):
        if isinstance(v, MutableMapping):
            return reduce(lambda a, x: self.__flatten_dict(x[1], keys + [x[0]], a), iteritems(v), acc)
        else:
            return acc + [('.'.join(keys), ':{0}'.format('_'.join(keys), v)]


class Collection(CollectionEngine):

    def __init__(self, name, table_name, **kwargs):
        self.__table = boto3.resource('dynamodb', **kwargs).Table(table_name)
    
    @property
    def table(self):
        return self.__table

    def create_raw_item(self, index, data_dict, context):
        if context is None:
            self.__table.put_item(Item=data_dict)
        else:
            context.put_item(self.__table, data_dict)
        return data_dict

    def retrieve_raw_item(self, key_dict):
        response = self.__table.get_item(Key=key_dict)
        if 'Items' not in response:
            raise KeyError(key_dict)
        return response['Items'][0]

    def query_raw_items(self, index, parent_key_value, **kwargs):
        if parent_key_value is not None:
            kwargs['KeyConditionExpression'] = Key(parent_key_value[0]).eq(parent_key_value[1])
        return self.__table.query(IndexName=index, **kwargs)['Items']

    def bulk_get_raw_items(self, **kwargs):
        return self.__table.batch_get(**kwargs)

    @classmethod
    def get_context(cls, *args, **kwargs):
        return Context()
