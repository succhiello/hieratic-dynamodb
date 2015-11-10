import time
from datetime import datetime

from six import string_types

from pytest import fixture

from voluptuous import Optional, All, Any, Range, Coerce

from typedtuple import TypedTuple

from boto.dynamodb2 import connect_to_region
from boto.dynamodb2.table import Table
from boto.dynamodb2.fields import HashKey, RangeKey, AllIndex, GlobalAllIndex
from boto.dynamodb2.types import NUMBER
from boto.exception import JSONResponseError

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
    return connect_to_region(ddb_region, host=ddb_host, is_secure=False, port=ddb_port)


def make_table(ddb, name, **kwargs):

    table = Table(table_name=name, connection=ddb, **kwargs)

    while True:
        try:
            if table.describe()['Table']['TableStatus'] == 'ACTIVE':
                return table
            else:
                time.sleep(1)
        except JSONResponseError as exc:
            if exc.error_code == 'ResourceNotFoundException':
                table = Table.create(table_name=name, connection=ddb, **kwargs)
            else:
                raise


@fixture(scope='module')
def organization_table(request, ddb):

    table = make_table(
        ddb,
        'HieraticDynamoDBTestOrganization',
        schema=[HashKey('id', data_type=NUMBER)]
    )

    def fin():
        table.delete()
    request.addfinalizer(fin)
    return table


@fixture(scope='module')
def user_table(request, ddb):

    table = make_table(ddb, 'HieraticDynamoDBTestUser', schema=[
        HashKey('organization_id', data_type=NUMBER),
        RangeKey('id', data_type=NUMBER)],
        indexes=[AllIndex('CreatedAtIndex', parts=[
            HashKey('organization_id', data_type=NUMBER),
            RangeKey('created_at', data_type=NUMBER),
        ])],
        global_indexes=[GlobalAllIndex('NameIndex', parts=[
            HashKey('name'),
            RangeKey('id', data_type=NUMBER),
        ])],
    )

    def fin():
        table.delete()
    request.addfinalizer(fin)
    return table


@fixture
def Organization():
    return TypedTuple('Organization', {'id': IdSchema, Optional('name'): six_string, Optional('created_at'): dt})


@fixture
def User():
    return TypedTuple('User', {'organization_id': IdSchema, 'id': IdSchema, Optional('name'): six_string, Optional('created_at'): dt})


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
