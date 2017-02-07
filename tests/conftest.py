import time
from datetime import datetime

from six import string_types

from pytest import fixture

from voluptuous import Optional, All, Any, Range, Coerce

from typedtuple import typedtuple

import boto3

from hieratic.item import ItemResource

IdSchema = All(Coerce(int), Range(min=0))


def six_string(v):
    if not isinstance(v, string_types):
        raise ValueError
    return v


dt = Any(datetime, datetime.fromtimestamp)


def pytest_addoption(parser):
    parser.addoption('--ddb-region', default='us-east-1')
    parser.addoption('--ddb-host', default='localhost')
    parser.addoption('--ddb-port', default=8000)


@fixture(scope='module')
def ddb_region(request):
    return request.config.getoption('--ddb-region')


@fixture(scope='module')
def ddb_host(request):
    return request.config.getoption('--ddb-host')


@fixture(scope='module')
def ddb_port(request):
    return request.config.getoption('--ddb-port')


@fixture(scope='module')
def ddb(ddb_region, ddb_host, ddb_port):
    return boto3.resource('dynamodb', endpoint_url='http://{}:{}'.format(ddb_host, ddb_port))


def make_table(ddb, name, **kwargs):

    ddb.create_table(
        TableName=name,
        **kwargs,
    )

    table = ddb.Table(name)
    table.wait_until_exists()
    return table


@fixture(scope='module')
def organization_table(request, ddb):

    table = make_table(
        ddb,
        'HieraticDynamoDBTestOrganization',
        AttributeDefinitions=[{
            'AttributeName': 'id',
            'AttributeType': 'N',
        }],
        KeySchema=[{
            'KeyType': 'HASH',
            'AttributeName': 'id',
        }],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5,
        },
    )

    def fin():
        table.delete()
    request.addfinalizer(fin)
    return table


@fixture(scope='module')
def user_table(request, ddb):

    table = make_table(
        ddb,
        'HieraticDynamoDBTestUser',
        AttributeDefinitions=[{
            'AttributeName': 'organization_id',
            'AttributeType': 'N',
        }, {
            'AttributeName': 'id',
            'AttributeType': 'N',
        }, {
            'AttributeName': 'created_at',
            'AttributeType': 'N',
        }, {
            'AttributeName': 'name',
            'AttributeType': 'S',
        }],
        KeySchema=[{
            'KeyType': 'HASH',
            'AttributeName': 'organization_id',
        }, {
            'KeyType': 'RANGE',
            'AttributeName': 'id',
        }],
        LocalSecondaryIndexes=[{
            'IndexName': 'CreatedAtIndex',
            'KeySchema': [{
                'KeyType': 'HASH',
                'AttributeName': 'organization_id',
            }, {
                'KeyType': 'RANGE',
                'AttributeName': 'created_at',
            }],
            'Projection': {
                'ProjectionType': 'ALL',
            }
        }],
        GlobalSecondaryIndexes=[{
            'IndexName': 'NameIndex',
            'KeySchema': [{
                'KeyType': 'HASH',
                'AttributeName': 'name',
            }, {
                'KeyType': 'RANGE',
                'AttributeName': 'id',
            }],
            'ProvisionedThroughput': {
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5,
            },
            'Projection': {
                'ProjectionType': 'ALL',
            },
        }],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5,
        },
    )

    def fin():
        table.delete()
    request.addfinalizer(fin)
    return table


@fixture
def Organization():
    return typedtuple('Organization', {'id': IdSchema, Optional('name'): six_string, Optional('created_at'): dt})


@fixture
def User():
    return typedtuple('User', {'organization_id': IdSchema, 'id': IdSchema, Optional('name'): six_string, Optional('created_at'): dt})


@fixture
def UserResource(User):

    @ItemResource.define(
        data_class=User,
        converters={
            'dynamodb': {
                'created_at': lambda dt: time.mktime(dt.timetuple()) + dt.microsecond / 1e6,
            },
        },
    )
    class UserRes(ItemResource):
        pass

    return UserRes
