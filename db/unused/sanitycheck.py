import logging
import traceback
from io import StringIO

import os, sys, inspect
from sqlalchemy import inspect as sqlaInspect
from sqlalchemy.ext.declarative.clsregistry import _ModuleMarker
from sqlalchemy.orm import RelationshipProperty


#logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DEBUG=True


#
#
#
def is_sane_database(Base, session):
    """Check whether the current database matches the models declared in model base.

    Currently we check that all tables exist with all columns. What is not checked

    * Column types are not verified

    * Relationships are not verified at all (TODO)

    :param Base: Declarative Base for SQLAlchemy models to check

    :param session: SQLAlchemy session bound to an engine

    :return: True if all declared models have corresponding tables and columns.
    """

    engine = session.get_bind()
    if DEBUG:
        print("\n################### dir engine:%s\n\n" % dir(engine))
    iengine = sqlaInspect(engine)
    if DEBUG:
        print("\n################### dir iengine:%s\n\n" % dir(iengine))

    errors = False

    tables = iengine.get_table_names()
    #relationships = iengine.relationships

    # Go through all SQLAlchemy models
    problems = []
    for name, klass in Base._decl_class_registry.items():
        if DEBUG:
            print("\n\n is_sane_database check name:%s; class:%s" % (name, klass))

        if isinstance(klass, _ModuleMarker):
            # Not a model
            continue

        if DEBUG:
            print(" > check klass:%s" % (klass))
        #foreign_key_set = klass.__table__.c.column.foreign_keys
        #print(" > check klass foreign_key_set:%s" % (foreign_key_set))

        i = sqlaInspect(klass)
        if DEBUG:
            print(" #################### i:%s" % i)
        #referred_class = [r.mapper.class_ for r in i.relationships]
        #print(" #################### referred_class:%s" % referred_class)


        table = klass.__tablename__
        if table in tables:
            print(" >> check table:%s" % (table))

            #i = sqlaInspect(table)
            #print(" #################### i:%s" % i)

            # Check all columns are found
            # Looks like [{'default': "nextval('sanity_check_test_id_seq'::regclass)", 'autoincrement': True, 'nullable': False, 'type': INTEGER(), 'name': 'id'}]

            columns = [c["name"] for c in iengine.get_columns(table)]
            if DEBUG:
                print(" > columns:%s" % (columns))
            mapper = sqlaInspect(klass)
            if DEBUG:
                print(" > mapper:%s" % (mapper))

            foreign_key_set = iengine.get_foreign_keys(table)
            if DEBUG:
                print(" > column foreign_key_set:%s" % (foreign_key_set))
                print(" > mapper.attrs:%s" % (mapper.attrs))

            for column_prop in mapper.attrs:
                #print(" ######## @@@@@@@ ###### do column_prop:%s; type;%s; dir:%s" % (column_prop, type(column_prop), dir(column_prop)))
                if DEBUG:
                    print(" ## do column_prop:%s" % (column_prop))

                if isinstance(column_prop, RelationshipProperty):
                    if DEBUG:
                        print("  > column_prop is relation")
                    # TODO: Add sanity checks for relations

                    #print(" ######## @@@@@@@ ###### do column.local_columns:%s" % (column_prop.local_columns))

                    if len(foreign_key_set)>0:
                        for relation in foreign_key_set:
                            #print(" ### HAHAHA ### relation.constrained_columns:%s; type:%s" % (relation.constrained_columns, type(relation.constrained_columns)))
                            if DEBUG:
                                print(" ### HAHAHA ### relation:%s; type:%s" % (relation, type(relation)))
                        #os._exit(1)
                    # GL: do some test on Relationship
                    #print(" ##################### column_prop:%s; RelationshipProperty:%s" % (column_prop, RelationshipProperty))
                    #print(" ##################### dir:%s\n ###### parent:%s" % (dir(RelationshipProperty), column_prop.mro()))
                    #print(" ##################### dir:%s\n ###### parent:%s" % (dir(RelationshipProperty), dir(column_prop)))
                    if 1==2:
                        try:
                            print(" #### HAHAHA:%s; %s" % (type(column_prop._user_defined_foreign_keys), column_prop._user_defined_foreign_keys))
                            for jj in column_prop._user_defined_foreign_keys:
                                print(" ##:%s" % jj)
                        except:
                            print(" ##################### TEST:ERROR")
                            traceback.print_exc(file=sys.stdout)

                    try:
                        if DEBUG:
                            print("  ## Relationship TEST:")
                        n=0
                        if len(column_prop._user_defined_foreign_keys) > 0:
                            for jj in column_prop._user_defined_foreign_keys:
                                if DEBUG:
                                    print("    ## Relationship TEST _user_defined_foreign_keys[%s]:%s" % (n,jj))
                                n+=1
                        else:
                            if DEBUG:
                                print("    ## Relationship TEST no _user_defined_foreign_keys")
                        n=0
                        if len(column_prop._calculated_foreign_keys) > 0:
                            for jj in column_prop._calculated_foreign_keys:
                                if DEBUG:
                                    print("    ## Relationship TEST _calculated_foreign_keys[%s]:%s" % (n,jj))
                                n+=1
                        else:
                            if DEBUG:
                                print("    ## Relationship TEST no _calculated_foreign_keys")
                    except:
                        if DEBUG:
                            print(" ## Relationship TEST:ERROR")
                        traceback.print_exc(file=sys.stdout)
                    pass
                else:
                    if DEBUG:
                        print("  > column_prop is not relation")
                    for column in column_prop.columns:
                        if DEBUG:
                            print(" ###### test no relation column '%s'" % (column))
                        # Assume normal flat column
                        if not column.key in columns:
                            if DEBUG:
                                print("  NOT normal column:%s; type:%s" % (column, type(columns)))
                            #
                            found = False
                            if len(foreign_key_set) > 0:
                                for relation in foreign_key_set:
                                    #print(" ### HAHAHA ### relation.constrained_columns:%s; type:%s" % (relation, type(relation)))
                                    #print(" ### HAHAHA ### relation.constrained_columns:%s; type:%s" % (relation['constrained_columns'], type(relation['constrained_columns'])))
                                    #print(" ### HAHAHA ### test relation '%s'; type;%s  VS %s" % (column.key, type(column.key), relation['constrained_columns']))
                                    for rel in relation['constrained_columns']:
                                        if DEBUG:
                                            print(" ### HAHAHA ### test relation '%s' VS '%s'" % (column.key, rel))
                                        if "%s_id" % column.key == rel:
                                            found = True
                                            if DEBUG:
                                                print("     HAHAHA ### relation found:%s" % rel)
                                            #os._exit(1)

                            if found:
                                if DEBUG:
                                    print(" ### relation found !!")
                            else:
                                if DEBUG:
                                    print(" ### relation not found !!")
                                    # GL: some test
                                    print("  #### @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ #### no match:%s dir:%s\n" % (column, dir(column)))
                                    print("  #### @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ #### toto:%s  ;  %s  ;  %s\n" % (column.base_columns, column.description, column.expression))

                                #for aa in columns:
                                    #print("    ######## NO key: column:%s; type:%s" % (aa, type(aa)))
                                    #print(" ######## NO key: column:%s; type:%s; %s" % (aa, type(aa), mapper.attrs[aa]))

                                logger.error("Model %s declares column %s which does not exist in database %s", klass, column.key, engine)
                                problems.append("Model %s declares column %s which does not exist in database %s" % (klass, column.key, engine))
                                errors = True
                        else:
                            if DEBUG:
                                print("  normal column:%s; type:%s" % (column, type(columns)))
            if DEBUG:
                print("")

        else:
            logger.error("Model %s declares table %s which does not exist in database %s", klass, table, engine)
            problems.append("Model %s declares table %s which does not exist in database %s" % (klass, table, engine))
            errors = True

    messagesOut = StringIO()
    print("\n\n>> error:%s" % errors)
    print(">> error:%s" % errors, file=messagesOut)
    if len(problems)>0:
        print("\n\n>> %s problem(s) found:" % len(problems))
        print(">> %s problem(s) found:" % len(problems), file=messagesOut)
        for problem in problems:
            print("  %s" % problem)
            print("  %s" % problem, file=messagesOut)
    else:
        print("\n\n>> no problem found.")

    return not errors, messagesOut.getvalue()