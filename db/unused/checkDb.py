#
# utility that:
# -  test that the model are correctly set in the DB: checkDbIntegrity
# -  init the DB: initDb
#
# use SQLAlchemy ORM
#
# Lavaux Gilles 2019-10
#
import os, sys
import traceback
from optparse import OptionParser
import datetime
#
#from db.sqlalchemyLayer import Base
#from sqlalchemy import Column, String, Text, Integer, BigInteger, Boolean, Date, DateTime, Table, ForeignKey
#
#import sqlalchemyLayer as dbLayer
#import databaseHelper as databaseHelper

from sqlalchemyLayer import databaseIsEmpty, createDbSchema, Session, Base, init
#import sanitycheck

#
# here all the model we want to use in the application:
#
#from models.management import Manager


#
#
#
def testEmpty():
    print(f"  database is empty: {databaseIsEmpty()}")

#
# create the db schemas for known models
#
def initDb():
    return createDbSchema()

#
# check that known models match the db schemas
#
def checkDbIntegrity():
    print(" starting db integrity check...")
    dbSesssion = Session()
    print("  got db session")
    #ok, mesg = sanitycheck.is_sane_database(Base, dbSesssion)
    ok, mesg = None
    dbSesssion.close()
    if ok:
        print("\n  db is sane")
        return "db is sane:\n %s" % mesg
    else:
        raise Exception("db is INCONSISTENT !!:\n%s" % mesg)

#
#
#
if __name__ == '__main__':
    dbConnectionString = None #'sqlite:////home/gilles/prism-check.db'
    action=None
    data=None

    options=[]
    parser = OptionParser()
    parser.add_option("-c", "--connectionString", dest="connectionString", help="db connection string")
    parser.add_option("-a", "--action", dest="action", default=False, help="action to be performed")
    parser.add_option("-d", "--data", dest="data", default=False, help="data")
    pOptions, args = parser.parse_args(sys.argv)

    if pOptions.connectionString != None:
        dbConnectionString=pOptions.connectionString
    else:
        raise Exception('need a db connection string (-c or --connectionString argument), try -h for syntax')

    if pOptions.data != None:
        data=pOptions.data

    if pOptions.action != None:
        action=pOptions.action
    else:
        raise Exception('need an action (-a or --action argument), try -h for syntax')

    print(" checkDb, will execute action %s on db at connectionString:%s" % (action, dbConnectionString))
    try:
        init('default', dbConnectionString)
        if action == 'checkDb':
            checkDbIntegrity()
        elif action == 'initDb':
            initDb()
        elif action == 'testEmpty':
            testEmpty()
        else:
            raise Exception("Unknown checkDb.process action:%s" % action)

    except SystemExit:
        print(" system exit")
        sys.exit(0)

    except KeyboardInterrupt:
        print(" interrupted by user")
        os._exit(0)

    except:
        print(" Error")
        exc_type, exc_obj, exc_tb = sys.exc_info()
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)
