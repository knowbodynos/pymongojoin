from pymongo import * 

class JoinedClient(mongo_client.MongoClient):
    def __init__(self, mongo_uri):
        super(self.__class__, self).__init__(mongo_uri)

    def __getattr__(self, key):
        if key not in self.__dict__:
            return JoinedDatabase(self, key)
        else:
            return self.__dict__[key]

    def __getitem__(self, key):
        if key not in self.__dict__:
            return JoinedDatabase(self, key)
        else:
            return self.__dict__[key]

class JoinedDatabase(database.Database):
    def __init__(self, client, database_name):
        super(self.__class__, self).__init__(client, database_name)

    def __getattr__(self, key):
        if key not in self.__dict__:
            return JoinedCollections(self, key)
        else:
            return self.__dict__[key]

    def __getitem__(self, key):
        if key not in self.__dict__:
            return JoinedCollections(self, key)
        else:
            return self.__dict__[key]

class JoinedCollections(object):
    class JoinedCollection(collection.Collection):
        def get_indexes(self):
            database = self._Collection__database
            collection_name = self._Collection__name
            curs = database.INDEXES.find({"TIER": collection_name}, {"_id": 0, "INDEX": 1, "INDEXID": 1}).sort([("INDEXID", 1)])
            return [x["INDEX"] for x in list(curs)]

    class JoinedCursor(object):
        def __init__(self, *args, **kwargs):
            self.__hint = None

            self.__skip = 0
            self.__limit = 0
            self.__counter = 0
            self.retrieved = 0

            self.__sort = None
            
            for k, v in kwargs["JoinedCollections"].items():
                self.__dict__[k] = v
            del kwargs["JoinedCollections"]

            args = list(args)
            if len(args) > 0:
                self.__find_filter = args[0]
                del args[0]
            elif "filter" in kwargs:
                self.__find_filter = kwargs["filter"]
                del kwargs["filter"]
            else:
                self.__find_filter = {}

            if len(args) > 0:
                self.__find_projection = args[0]
                del args[0]
            elif "projection" in kwargs:
                self.__find_projection = kwargs["projection"]
                del kwargs["projection"]
            else:
                self.__find_projection = {}
            if self.__find_projection and not "_id" in self.__find_projection:
                self.__find_projection["_id"] = 1

            self.__find_args = args
            self.__find_kwargs = kwargs

            self.__collection_info = {}
            for collection_name in self._JoinedCollections__seq:
                collection = self.__dict__[collection_name]
                sample_doc = list(collection.find(None, None, *self.__find_args, **kwargs).limit(1))[0]
                collection_fields = list(sample_doc.keys())
                collection_filter = {}
                collection_projection = {}
                #for field in collection_fields:
                #    if field in self.__find_filter:
                #        collection_filter[field] = self.__find_filter[field]
                #    if field in self.__find_projection:
                #        collection_projection[field] = self.__find_projection[field]
                for field in list(self.__find_filter.keys()):
                    if field[:len(collection_name) + 1] == collection_name + '.':
                        new_field = field[len(collection_name) + 1:]
                        self.__find_filter[new_field] = self.__find_filter.pop(field)
                        collection_filter[new_field] = self.__find_filter[new_field]

                for field in list(self.__find_projection.keys()):
                    if field[:len(collection_name) + 1] == collection_name + '.':
                        new_field = field[len(collection_name) + 1:]
                        self.__find_projection[new_field] = self.__find_projection.pop(field)
                        collection_projection[new_field] = self.__find_projection[new_field]

                self.__collection_info[collection_name] = {
                                                       "fields": collection_fields,
                                                       "indexes": collection.get_indexes(),
                                                       "filter": collection_filter,
                                                       "projection": collection_projection
                                                   }

            self.__cursor = self.__recursive_find(0, {})

        def __project_doc(self, doc):
            project_doc = {}
            for field in doc:
                if field in self.__find_projection and self.__find_projection[field]:
                    project_doc[field] = doc[field]
            return project_doc

        def __recursive_find(self, level, super_doc):
            if level < len(self._JoinedCollections__seq):
                collection_name = self._JoinedCollections__seq[level]
                collection = self.__dict__[collection_name]
                indexes = self.__collection_info[collection_name]["indexes"]

                filter_with_indexes = {}
                for field, val in self.__collection_info[collection_name]["filter"].items():
                    filter_with_indexes[field] = val
                for index in indexes:
                    if index in super_doc:
                        filter_with_indexes[index] = super_doc[index]

                projection_with_indexes = {}
                for field, val in self.__collection_info[collection_name]["projection"].items():
                    projection_with_indexes[field] = val
                for index in indexes:
                    projection_with_indexes[index] = 1

                cursor = collection.find(filter_with_indexes, projection_with_indexes, *self.__find_args, **self.__find_kwargs)

                if self.__hint:
                    hint_items = []
                    for field, val in self.__hint:
                        if field in self.__collection_info[collection_name]["filter"]:
                            hint_items.append((field, val))
                    if len(hint_items) > 0:
                        cursor.hint(hint_items)

                if self.__sort:
                    sort_items = []
                    for field, val in self.__sort:
                        if field in self.__collection_info[collection_name]["fields"]:
                            sort_items.append((field, val))
                    if len(sort_items) > 0:
                        cursor.sort(sort_items)
                
                #if cursor.count() == 0:
                #    yield self.__project_doc(super_doc)
                #else:
                if cursor.count() > 0:
                    for doc in cursor:
                        doc.update(super_doc)
                        for sub_doc in self.__recursive_find(level + 1, doc):
                            yield self.__project_doc(sub_doc)
            else:
                yield self.__project_doc(super_doc)

        def __iter__(self):
            for doc in self.__cursor:
                if self.__counter >= self.__skip:
                    break
                self.__counter += 1
            for doc in self.__cursor:
                if self.__limit > 0 and self.retrieved >= self.__limit:
                    raise StopIteration
                next_doc = doc
                self.__counter += 1
                self.retrieved += 1
                yield next_doc

        # def next(self):
        #     while self.__counter < self.__skip:
        #         self.__cursor.next()
        #         self.__counter += 1
        #     if self.__limit > 0 and self.retrieved >= self.__limit:
        #         raise StopIteration
        #     next_doc = self.__cursor.next()
        #     self.__counter += 1
        #     self.retrieved += 1
        #     return next_doc

        def __next__(self):
            while self.__counter < self.__skip:
                next(self.__cursor)
                self.__counter += 1
            if self.__limit > 0 and self.retrieved >= self.__limit:
                raise StopIteration
            next_doc = next(self.__cursor)
            self.__counter += 1
            self.retrieved += 1
            return next_doc

        def rewind(self):
            self.__counter = 0
            self.retrieved = 0
            self.__cursor = self.__recursive_find(0, {})

        def hint(self, index):
            self.__hint = index
            return self

        def skip(self, skip):
            self.__skip = skip
            return self

        def limit(self, limit):
            self.__limit = limit
            return self

        def sort(self, key_or_list, direction = None):
            if direction:
                key_or_list = [(key_or_list, direction)]
            self.__sort = key_or_list
            return self

        def count(self, with_limit_and_skip = False):
            total_count = 0
            if with_limit_and_skip:
                for doc in self.__cursor:
                    if self.__counter >= self.__skip:
                        break
            for doc in self.__cursor:
                if with_limit_and_skip and self.__limit > 0 and self.__counter >= self.__limit:
                    break
                total_count += 1
            return total_count

    def __init__(self, database = None, collection_name = None):
        self.__seq = []
        if database and collection_name:
            self.__dict__[collection_name] = self.JoinedCollection(database, collection_name)
            self.__seq.append(collection_name)

    def join(self, db_collections):
        assert(isinstance(db_collections, JoinedCollections))
        for collection_name in db_collections.__seq:
            self.__dict__[collection_name] = db_collections.__dict__[collection_name]
            self.__seq.append(collection_name)
        return self

    def get_indexes(self):
        indexes = []
        index_count = {}
        for collection_name in self.__seq:
            for index in self.__dict__[collection_name].get_indexes():
                if not index in indexes:
                    indexes.append(index)
                    index_count[index] = 1
                else:
                    index_count[index] += 1
        return [index for index in indexes if index_count[index] == len(self.__seq)]

        database = self._Collection__database
        collection_name = self._Collection__name
        curs = database.INDEXES.find({"TIER": collection_name}, {"_id": 0, "INDEX": 1, "INDEXID": 1}).sort([("INDEXID", 1)])
        return [x["INDEX"] for x in list(curs)]

    def find(self, *args, **kwargs):
        if len(self.__seq) == 0:
            raise Exception("No collections specified.")
        elif len(self.__seq) == 1:
            collection_name = self.__seq[0]
            collection = self.__dict__[collection_name]
            return collection.find(*args, **kwargs)
        else:
            kwargs.update({"JoinedCollections": self.__dict__})
            return self.JoinedCursor(*args, **kwargs)