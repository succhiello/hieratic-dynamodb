from six import itervalues
from collections import defaultdict


class Context(object):

    def __init__(self, flush_amount=25, overwrite_by_pkeys=None):
        self.__flush_amount = flush_amount
        self.__overwrite_by_pkeys = overwrite_by_pkeys or []
        self.__client = None
        self.__items_buffer = defaultdict(list)

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):

        if exc_type:
            return False
        
        while self.__items_buffer:
            self.__flush()
        
        return True
    
    def put_item(self, table, item):
        self.__set_client(table)
        self.__add_request_and_process(
            table.name,
            {'PutRequest': {'Item': item}},
            self.__overwrite_by_pkeys,
        )
    
    def delete_item(self, table, key):
        self.__set_client(table)
        self.__add_request_and_process(
            table.name,
            {'DeleteRequest': {'Key': key}},
            self.__overwrite_by_pkeys,
        )

    def __set_client(self, table):
        if self.__client is None:
            self.__client = table.meta.client

    def __add_request_and_process(self, table_name, request, pkeys):
        self.__items_buffer[table_name][:] = self.__remove_dup_pkeys_request_if_any(
            self.__items_buffer[table_name],
            request,
            pkeys,
        )
        self.__items_buffer[table_name].append(request)
        self.__flush_if_needed()

    def __remove_dup_pkeys_request_if_any(self, items, request, pkeys):
        if len(pkeys) == 0:
            return items
        pkey_values_new = self.__extract_pkey_values(request, pkeys)
        return [item for item in items if self.__extract_pkey_values(item, pkeys) != pkey_values_new]

    def __extract_pkey_values(self, request, pkeys):

        put_request = request.get('PutRequest', {}).get('Item')
        if put_request:
            return [put_request[key] for key in pkeys]

        delete_request = request.get('DeleteRequest', {}).get('Key')
        if delete_request:
            return [delete_request[key] for key in pkeys]

        return None

    def __flush_if_needed(self):
        if sum(
            len(table_requests)
            for table_requests in itervalues(self.__items_buffer)
        ) >= self.__flush_amount:
            self.__flush()

    def __flush(self):
        response = self.__client.batch_write_item(RequestItems=self.__items_buffer)
        self.__items_buffer = defaultdict(list, response.get('UnprocessedItems', {}))
