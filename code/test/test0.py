
# this code is from https://pynative.com/python-postgresql-tutorial/

from db_config import connection_parameters

import psycopg2
try:
    connection = psycopg2.connect(
#        user = "___DB_USER___",
#        password = "___DB_PASSWORD___",
#        host = "127.0.0.1",
#        port = "___DB_EXTERNAL_PORT___",
#        database = "deduplifier"
        **connection_parameters
    )

    cursor = connection.cursor()
    # Print PostgreSQL Connection properties
    print ( connection.get_dsn_parameters(),"\n")

    # Print PostgreSQL version
    cursor.execute("SELECT version();")
    record = cursor.fetchone()
    print("You are connected to - ", record,"\n")

except (Exception, psycopg2.Error) as error :
    print ("Error while connecting to PostgreSQL", error)
finally:
    #closing database connection.
    if(connection):
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")
