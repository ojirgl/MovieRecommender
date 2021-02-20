import json
import psycopg2
import pandas as pd

class QueryBuilder:

    def __init__(self, conf):
        self.conf = conf
        self.conn_string = QueryBuilder.connection_string(self.conf)
        

    @staticmethod
    def connection_string(conf):
        """
        dummy query to check if the report type already exists.
        """
        conn_string = ("dbname='{dbname}' "
                       "port='{port}' "
                       "user='{user}' "
                       "password='{pwd}' "
                       "host='{host_url}'").format(**conf)

        return conn_string

    @staticmethod
    def schema(conf):
        schema = conf['schema']+'.'

        return schema
    

    @staticmethod
    def database_schema(conf):
        database_schema = conf['dbname']+'.'+conf['schema']+'.'
        
        return database_schema

    @staticmethod
    def select(table, limit, *args, **kwargs):
        """ Generates SQL for a SELECT statement matching the kwargs passed. """
        sql = []
        sql.append("SELECT * FROM %s " % table)
        if args:
            sql.append("WHERE " + str(args[0]))
        if kwargs:
            sql.append("WHERE " + " AND ".join("%s = '%s'" % (k, v) for k, v in kwargs.items()))
        else:
            pass
        if limit != None:
            lim = ' limit {} '.format(limit)
#         elif not isinstance(limit, int):
#             raise ValueError('limit must be an integer')
        else:
            lim = ''
        sql.append(lim)
        sql.append(";")
        return "".join(sql)

    @staticmethod
    def insert(table, **kwargs):
        """ insert rows into table 
        given the key-value pairs in kwargs """
        keys = ["%s" % k for k in kwargs]
        values = ["'%s'" % v for v in kwargs.values()]
        sql = []
        sql.append("INSERT INTO %s (" % table)
        sql.append(", ".join(keys))
        sql.append(") VALUES (")
        sql.append(", ".join(values))
        # ONLY IN CASE OF UPSERT --> NOT SUPPORTED BY REDSHIFT
        #sql.append(") ON DUPLICATE KEY UPDATE ")
        #sql.append(", ".join("%s = '%s'" % (k, v) for k, v in kwargs.items()))
        sql.append(") ;")
        return "".join(sql)
    
    @staticmethod
    def update(table, dictionary_of_changes, **kwargs):
        """
        Update a table with given dictionary of modifications
        where **kwargs condition. Example: update(table='my_table', dictionary_of_changes="{'col1' : '55', 'col2' : '60'}", month='08', year='2019')
        """
        
        def new_fields(dictionary_of_changes):

            s = dictionary_of_changes.replace("'", "\"")
            d = json.loads(s)

            string_where = []

            for key, value in d.items():
                value = "'"+ value + "'"
                substr = f'{key}={value}'
                string_where.append(substr)
            return ' , '.join(string_where)

        def conditions(**kwargs):

            string_where = []

            for key, value in kwargs.items():
                value = "'"+ value + "'"
                substr = f'{key}={value}'
                string_where.append(substr)
            return ' and '.join(string_where)

        keys = ["%s" % k for k in kwargs]
        values = ["'%s'" % v for v in kwargs.values()]

        sql = []
        sql.append("UPDATE %s " % table)
        sql.append("SET " + new_fields(dictionary_of_changes))
        sql.append(" WHERE " + conditions(**kwargs))
        sql.append(" ;")
        return "".join(sql)

    @staticmethod
    def delete(table, **kwargs):
        """ deletes rows from table where **kwargs """
        sql = []
        sql.append("DELETE FROM %s " % table)
        sql.append("WHERE " + " AND ".join("%s = '%s'" % (k, v) for k, v in kwargs.items()))
        sql.append(";")
        return "".join(sql)

    @staticmethod
    def copy_redshift(table_path, s3_path, id_key, secret_key, zip_format, **kwargs):
    
        sql = []
        sql.append("copy %s " % table_path)
        sql.append("from 's3://" + s3_path+"' ")
        sql.append("credentials 'aws_access_key_id={};aws_secret_access_key={}' ".format(id_key,secret_key))
        sql.append(" ".join("%s %s" % (k, v) for k, v in kwargs.items()))
        if zip_format != None:
            sql.append(" " + zip_format)
        else:
            pass
        sql.append("; commit;")
        return "".join(sql)

def execute_query(conn_string, query, query_type):

    valid_types = {'select', 'insert', 'delete', 'update', 'copy_redshift', 'other'}
    if query_type not in valid_types:
        raise ValueError("results: status must be one of %r." % valid_types)

    if query_type == 'select':

        try:
            with psycopg2.connect(conn_string) as connection:
                try:
                    df = pd.read_sql_query(query, connection)
                except:
                    df = pd.DataFrame()
                finally:
                    return df
        except:
            print("Unable to connect to Redshift")
            return
    else:
        try:
            with psycopg2.connect(conn_string) as connection:
                try:
                    cursor = connection.cursor()
                    cursor.execute(query)
                except:
                    print('Execution failed')
                finally:
                    return
        except:
            print("Unable to connect to Redshift")
            return
