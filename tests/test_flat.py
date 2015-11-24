from pytest import fixture, raises

from datetime import datetime

from hieratic.collection import CollectionResource
from hieratic.index import SimpleIndex


@fixture
def UsersResource(UserResource, ddb_region, ddb_host, ddb_port):

    @CollectionResource.define(
        item_class=UserResource,
        primary_index=SimpleIndex(('organization_id', int), ('id', int)),
    )
    class UsersRes(CollectionResource):
        def __init__(self):
            CollectionResource.__init__(self, None, 'users', 'dynamodb', 'HieraticDynamoDBTestUser', ddb_region, ddb_host, False, ddb_port)

    return UsersRes


class TestFlat(object):

    def test_flat(self, user_table, UsersResource, User):

        users_resource = UsersResource()

        now = datetime.now()

        user_resource = users_resource.create(User(organization_id=0, id=0, created_at=now))
        user = user_resource.data
        assert user.organization_id == 0
        assert user.id == 0
        assert user.created_at == now

        user_resource = users_resource['0_0']
        user = user_resource.data
        assert user.organization_id == 0
        assert user.id == 0
        assert user.created_at == now

        user_ressource = users_resource.retrieve(0, 0)
        user = user_resource.data
        assert user.organization_id == 0
        assert user.id == 0
        assert user.created_at == now

        user_resource.update(name='updated')
        user = user_resource.data
        assert user.name == 'updated'

        user_resource.delete()
        user = user_resource.data
        assert user is None

        with raises(KeyError):
            users_resource['0_0']

        with CollectionResource.get_context('dynamodb') as context:
            users_resource.create(User(organization_id=0, id=1), context)
            users_resource.create(User(organization_id=0, id=2), context)
            users_resource.create(User(organization_id=0, id=3), context)
            assert len(list(users_resource.query(organization_id__eq=0))) == 0

        assert [1, 2, 3] == [u_res.data.id for u_res in users_resource.query(organization_id__eq=0, reverse=True)]

        assert [1, 3] == [
            u_res.data.id for u_res in
            users_resource.bulk_get(keys=[{'organization_id': 0, 'id': 1},
                                          {'organization_id': 0, 'id': 3}])
        ]
