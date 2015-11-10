from boto.dynamodb2.items import Item


class Context(object):

    def __init__(self):
        self.__batch_data = {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):

        if exc_type:
            return False

        # Flush anything that's left.
        if self.__batch_data:
            self.flush()

        return True

    def put_item(self, table, data):
        if table not in self.__batch_data:
            self.__batch_data[table] = {'put': [], 'delete': []}

        self.__batch_data[table]['put'].append(data)

        if self.should_flush():
            self.flush()

    def delete_item(self, table, **kwargs):
        if table not in self.__batch_data:
            self.__batch_data[table] = {'put': [], 'delete': []}

        self.__batch_data[table]['delete'].append(kwargs)

        if self.should_flush():
            self.flush()

    def should_flush(self):
        return sum(
            len(items)
            for table_info in self.__batch_data.values()
            for items in table_info.values()
        ) == 25

    def flush(self):
        batch_data = {}
        connection = None
        for table, items in self.__batch_data.iteritems():
            if connection is None:
                connection = table.connection
            batch_data[table.table_name] = [
                {
                    'PutRequest': {
                        'Item': Item(table, data=item).prepare_full()
                    }
                } for item in items['put']
            ] + [
                {
                    'DeleteRequest': {
                        'Key': table._encode_keys(item)
                    }
                } for item in items['delete']
            ]
        while batch_data:
            batch_data = connection.batch_write_item(batch_data).get(
                'UnprocessedItems',
                {}
            )
        self.__batch_data = {}
        return True
