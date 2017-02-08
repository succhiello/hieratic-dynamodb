from pytest import fixture, raises

from time import mktime
from datetime import datetime

from boto3.dynamodb.conditions import Key

from hieratic import Resource
from hieratic.item import ItemResource
from hieratic.collection import CollectionResource
from hieratic.index import SimpleIndex


@fixture
def UsersResource(UserResource, ddb_region, ddb_host, ddb_port):

    @CollectionResource.define(
        item_class=UserResource,
        primary_index=SimpleIndex(('organization_id', int), ('id', int)),
    )
    class UsersRes(CollectionResource):
        def __init__(self, parent, name):
            CollectionResource.__init__(
                self,
                parent,
                name,
                'dynamodb',
                'HieraticDynamoDBTestUser',
                region_name=ddb_region,
                use_ssl=False,
                endpoint_url='{}:{}'.format(ddb_host, ddb_port),
            )

    return UsersRes


@fixture
def OrganizationResource(Organization, UsersResource):

    @ItemResource.define(
        data_class=Organization,
        converters={
            'dynamodb': {
                'created_at': lambda dt: mktime(dt.timetuple()) + dt.microsecond / 1e6,
            },
        },
        child_definitions={'users': UsersResource},
    )
    class OrganizationRes(ItemResource):
        pass

    return OrganizationRes


@fixture
def OrganizationsResource(OrganizationResource, ddb_region, ddb_host, ddb_port):

    @CollectionResource.define(
        item_class=OrganizationResource,
        primary_index=SimpleIndex(('id', int),)
    )
    class OrganizationsRes(CollectionResource):
        def __init__(self, parent, name):
            CollectionResource.__init__(
                self,
                parent,
                name,
                'dynamodb',
                'HieraticDynamoDBTestOrganization',
                region_name=ddb_region,
                use_ssl=False,
                endpoint_url='{}:{}'.format(ddb_host, ddb_port),
            )

    return OrganizationsRes


@fixture
def RootResource(OrganizationsResource):

    @Resource.children({'organizations': OrganizationsResource})
    class RootRes(Resource):
        pass

    return RootRes


def test_hierarchy(organization_table, user_table, RootResource, Organization, User):

    root_res = RootResource(None, 'root')
    organization_res = root_res['organizations'].create(Organization(id=0))
    organization_res['users'].create(User(organization_id=0, id=0))

    user = root_res['organizations'][0]['users'][0].data
    assert user.organization_id == 0
    assert user.id == 0

    with raises(ValueError):
        organization_res['users'].create(User(organization_id=1, id=0))

    root_res['organizations'][0]['users'][0].delete()
    with raises(KeyError):
        root_res['organizations'][0]['users'][0]


def test_query(organization_table, user_table, RootResource, Organization, User):

    root_res = RootResource(None, 'root')
    organization_res = root_res['organizations'][0]
    organization_res['users'].create(User(
        organization_id=0,
        id=0,
        name='test',
        created_at=datetime(2015, 1, 1),
    ))
    organization_res['users'].create(User(
        organization_id=0,
        id=1,
        created_at=datetime(1970, 1, 1),
    ))
    organization_res['users'].create(User(
        organization_id=0,
        id=2,
        name='test',
        created_at=datetime(2050, 1, 1),
    ))

    assert len(list(organization_res['users'].query())) == 3
    assert [u.data.id for u in organization_res['users'].query(index='CreatedAtIndex', KeyConditionExpression=Key('organization_id').eq(0))] == [1, 0, 2]
    assert [u.data.id for u in organization_res['users'].query(index='NameIndex', KeyConditionExpression=Key('name').eq('test'), ScanIndexForward=False)] == [2, 0]
