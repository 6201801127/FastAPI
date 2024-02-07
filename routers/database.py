import psycopg2
from decouple import config


# try:
#     engine = psycopg2.connect(database=config("db_name"), user=config("db_user"), password=config("db_pass"), host=config("db_host"), port=config("db_port"))
#     engine = psycopg2.connect(database="bsap_3_nov", user="odoo", password="odoo", host="127.0.0.1", port="5432")
#
#
# except (Exception, psycopg2.DatabaseError) as error:
#     print(error)
    # if connection is not None:
    #     connection.close()
    #     print('Database connection closed.')


def connectToDB():
    try:
        engine = psycopg2.connect(database="bsap_db_v5", user="postgres", password="root", host="127.0.0.1", port="5432")
        # engine = psycopg2.connect(database=config("db_name"), user=config("db_user"), password=config("db_pass"), host=config("db_host"), port=config("db_port"))
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    else:
        return engine
