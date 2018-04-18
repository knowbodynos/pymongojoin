# pymongojoin
A Python package based on PyMongo that joins collections with shared indexes.

------------------------------------------------------------------------------------------------------------

Installation instructions:

1) Download the `pymongojoin` package

```
   git clone https://github.com/knowbodynos/pymongojoin.git
```

2) Navigate into the main directory

```
   cd pymongojoin
```

3) Install `pymongojoin`

```
   python setup.py install
```

------------------------------------------------------------------------------------------------------------

Using `pymongojoin`:

1) Create collections, as you would in PyMongo:

```
   client = pymongojoin.dbClient(<MONGO_URI>)
   database = client.<MONGO_DATABASE_NAME>
   collection_1 = database.<MONGO_COLLECTION_1>
   collection_2 = database.<MONGO_COLLECTION_1>
   collection_3 = database.<MONGO_COLLECTION_1>
```

2) Join collections together:

```
   collections = collection_1.join(collection_2).join(collection_3)
```

3) Find documents results in a cursor:

```
   cursor = collections.find(<FILTER>, <PROJECTION>, <OPTIONAL_ARGUMENTS>)
```

4) Add a hint, sort, limit, skip, count to the cursor:

```
   cursor.hint(<ARRAY_OF_KEY/ORDER_TUPLES>)
   cursor.skip(<INTEGER>)
   cursor.limit(<INTEGER>)
   cursor.sort(<ARRAY_OF_KEY/ORDER_TUPLES>)
   cursor.count()
```

5) Iterate through cursor:

```
   cursor.next()
```

or

```
   for doc in cursor:
       ...
```

6) Rewind cursor:

```
   cursor.rewind()
```