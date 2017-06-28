from collections import MutableMapping

from six import iteritems
from six.moves import reduce

from boto3.session import Session
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

from hieratic.engine import ItemEngine, CollectionEngine

from hieratic_dynamodb.context import Context


class Item(ItemEngine):

    def __init__(self, collection, raw_item):
        ItemEngine.__init__(self, collection, raw_item)
        self.__item = raw_item

    def update(self, index, patch, context, updates):
        if patch:
            self.collection.table.update_item(
                Key=index.make_key_dict_from_dict(self.__item),
                **Item.__make_update_expression_args(updates),
            )
            Item.__deep_update(self.__item, updates)
        else:
            if context is None:
                self.collection.table.put_item(Item=updates)
            else:
                context.put_item(self.collection.table, updates)
            self.__item = updates

    def delete(self, index, context):
        if context is None:
            self.collection.table.delete_item(Key=index.make_key_dict_from_dict(self.__item))
        else:
            context.delete_item(
                self.collection.table,
                index.make_key_dict_from_dict(self.get_dict()),
            )
        self.__item = None

    def get_dict(self):
        return self.__item

    @staticmethod
    def __make_update_expression_args(updates):
        flattened = Item.__flatten_dict(updates, [], [])
        return {
            'UpdateExpression': 'SET ' + ', '.join(['#{0} = :{1}'.format('.#'.join(x[0]), '_'.join(x[0])) for x in flattened]),
            'ExpressionAttributeValues': dict((':' + '_'.join(x[0]), x[1]) for x in flattened),
            'ExpressionAttributeNames': dict(('#{}'.format(x), x) for x in set(key for i in flattened for key in i[0])),
        }

    @staticmethod
    def __flatten_dict(v, keys, acc):
        if isinstance(v, MutableMapping):
            return reduce(lambda a, x: Item.__flatten_dict(x[1], keys + [x[0]], a), iteritems(v), acc)
        else:
            return acc + [(keys, v)]
    
    @staticmethod
    def __deep_update(dst, src):
        for k, v in iteritems(src):
            if isinstance(v, MutableMapping):
                dst[k] = Item.__deep_update(dst.get(k, {}), v)
            else:
                dst[k] = src[k]
        return dst


class Collection(CollectionEngine):

    def __init__(self, name, table_name, boto3_dynamodb_resource):
        self.__table = boto3_dynamodb_resource.Table(table_name)

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
        if 'Item' not in response:
            raise KeyError(key_dict)
        return response['Item']

    def query_raw_items(self, index, parent_key_value, **kwargs):
        if parent_key_value is not None:
            kwargs['KeyConditionExpression'] = Key(parent_key_value[0]).eq(parent_key_value[1])
        if index is not None:
            kwargs['IndexName'] = index
        return self.__table.query(**kwargs)['Items']

    def bulk_get_raw_items(self, **kwargs):
        return self.__table.meta.client.batch_get_item(
            RequestItems={self.__table.name: kwargs}
        ).get('Responses', {}).get(self.__table.name, [])

    @classmethod
    def get_context(cls, *args, **kwargs):
        return Context(*args, **kwargs)
