#
#
#
import os, sys, traceback
import sqlalchemyLayer as dbLayer


#
#
#
def dbIsEmpty(dbUrl):
    print(" dbIsEmpty check")
    print(" databaseHelper.dbIsEmpty: dbLayer:%s; engine=%s" % (dbLayer, dbLayer.engine))
    if dbLayer.engine==None:
        print(" databaseHelper.dbIsEmpty 0: engine is None; do an init")
        dbLayer.init('default', dbUrl)
        print(" databaseHelper.dbIsEmpty 1: engine=%s" % (dbLayer.engine))
    try:
        empty = dbLayer.databaseIsEmpty()
        print(" databaseHelper.dbIsEmpty 2 empty check:%s" % empty)
        return empty
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
        raise(e)


#
#
#
def ensureDbNotEmpty(dbUrl):
    print(" ensureDbNotEmpty check")
    print(" databaseHelper.ensureDbNotEmpty 0: dbLayer:%s; engine=%s" % (dbLayer, dbLayer.engine))
    if dbLayer.engine==None:
        print(" databaseHelper.ensureDbNotEmpty 1: engine is None; do an init")
        dbLayer.init('default', dbUrl)
    print(" databaseHelper.ensureDbNotEmpty 2: dbLayer:%s; engine=%s" % (dbLayer, dbLayer.engine))

    if dbIsEmpty(dbUrl):
        print(" databaseHelper.ensureDbNotEmpty 3 db is empty")
        createDbSchema()
    else:
        print(" databaseHelper.ensureDbNotEmpty 4: db is NOT empty")


#
#
#
def createDbSchema():
    print(" databaseHelper.createDbSchema: dbLayer:%s; engine=%s" % (dbLayer, dbLayer.engine))
    # - generate database schema for all known
    dbLayer.Base.metadata.create_all(dbLayer.engine)
    print(" databaseHelper.createDbSchema; schema created")

