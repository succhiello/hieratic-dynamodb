from pytest import fixture, raises

from datetime import datetime

from boto3.dynamodb.conditions import Key

from hieratic.collection import CollectionResource
from hieratic.index import SimpleIndex


@fixture
def UsersResource(UserResource, ddb):

    @CollectionResource.define(
        item_class=UserResource,
        primary_index=SimpleIndex(('organization_id', int), ('id', int)),
    )
    class UsersRes(CollectionResource):
        def __init__(self):
            CollectionResource.__init__(
                self, None,
                'users',
                'dynamodb',
                'HieraticDynamoDBTestUser',
                ddb,
            )

    return UsersRes


class TestFlat(object):

    def test_flat(self, user_table, UsersResource, User):

        users_resource = UsersResource()

        now = datetime.now()

        user_resource = users_resource.create(User(organization_id=0, id=0, created_at=now, phone={
            'home': '00011112222',
            'work': '33344445555',
        }))
        user = user_resource.data
        assert user.organization_id == 0
        assert user.id == 0
        assert user.created_at == now
        assert user.phone == {
            'home': '00011112222',
            'work': '33344445555',
        }

        user_resource = users_resource['0_0']
        user = user_resource.data
        assert user.organization_id == 0
        assert user.id == 0
        assert user.created_at == now
        assert user.phone == {
            'home': '00011112222',
            'work': '33344445555',
        }

        user_ressource = users_resource.retrieve(0, 0)
        user = user_resource.data
        assert user.organization_id == 0
        assert user.id == 0
        assert user.created_at == now
        assert user.phone == {
            'home': '00011112222',
            'work': '33344445555',
        }

        user_resource.update(name='updated', phone={'work': '66677778888'})
        user = user_resource.data
        assert user.name == 'updated'
        assert user.phone == {
            'home': '00011112222',
            'work': '66677778888',
        }

        raw_user = users_resource.engine.table.get_item(Key={
            'organization_id': 0,
            'id': 0,
        })['Item']
        assert raw_user['name'] == 'updated'
        assert raw_user['phone'] == {
            'home': '00011112222',
            'work': '66677778888',
        }

        user_resource.delete()
        user = user_resource.data
        assert user is None

        with raises(KeyError):
            users_resource['0_0']

        with CollectionResource.get_context('dynamodb') as context:
            users_resource.create(User(organization_id=0, id=1), context)
            users_resource.create(User(organization_id=0, id=2), context)
            users_resource.create(User(organization_id=0, id=3), context)
            assert len(list(users_resource.query(KeyConditionExpression=Key('organization_id').eq(0)))) == 0

        user_resources = [u_res for u_res in users_resource.query(KeyConditionExpression=Key('organization_id').eq(0))]
        assert [1, 2, 3] == [u_res.data.id for u_res in user_resources]

        assert [1, 3] == sorted(
            u_res.data.id for u_res in
            users_resource.bulk_get(Keys=[{'organization_id': 0, 'id': 1},
                                          {'organization_id': 0, 'id': 3}])
        )

        with CollectionResource.get_context('dynamodb') as context:
            for u_res in user_resources:
                u_res.delete(context)
            assert len(list(users_resource.query(KeyConditionExpression=Key('organization_id').eq(0)))) == 3
        assert len(list(users_resource.query(KeyConditionExpression=Key('organization_id').eq(0)))) == 0