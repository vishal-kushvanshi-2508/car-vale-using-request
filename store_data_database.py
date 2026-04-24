

from typing import List, Tuple
import mysql.connector # Must include .connector


table_name = "cars_url"
DB_CONFIG = {
    "host" : "localhost",
    "user" : "root",
    "password" : "actowiz",
    "port" : "3306",
    "database" : "car_vale_request_db"
}

def get_connection():
    try:
        ## here ** is unpacking DB_CONFIG dictionary.
        connection = mysql.connector.connect(**DB_CONFIG)
        ## it is protect to autocommit
        connection.autocommit = False
        return connection
    except Exception as e:
        print(f"Database connection failed: {e}")
        raise

def create_db():
    connection = get_connection()
    # connection = mysql.connector.connect(**DB_CONFIG)
    cursor = connection.cursor()
    cursor.execute("CREATE DATABASE IF NOT EXISTS car_vale_request_db;")
    connection.commit()
    connection.close()
# create_db()


def create_cars_url_url_table():
    connection = get_connection()
    cursor = connection.cursor()
    try:
        query =  f"""
                CREATE TABLE IF NOT EXISTS {table_name}(
                id INT AUTO_INCREMENT PRIMARY KEY,
                brand_name VARCHAR(200),
                brand_url TEXT,
                car_name VARCHAR(200),
                car_url TEXT,
                status VARCHAR(200)
        ); """
        cursor.execute(query)
        connection.commit()
    except Exception as e:
        print("Table creation failed")
        if connection:
            connection.rollback()
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

batch_size_length = 100
def data_commit_batches_wise(connection, cursor, sql_query : str, sql_query_value: List[Tuple], batch_size: int = batch_size_length ):
    ## this is save data in database batches wise.
    batch_count = 0
    for index in range(0, len(sql_query_value), batch_size):
        batch = sql_query_value[index: index + batch_size]
        cursor.executemany(sql_query, batch)
        batch_count += 1
        connection.commit()
    return batch_count


def insert_cars_url_table(list_data : list):
    connection = get_connection()
    cursor = connection.cursor()
    if not list_data:
        return
    dict_data = list_data[0]
    columns = ", ".join(list(dict_data.keys()))
    values = "".join([len(dict_data.keys()) * '%s,']).strip(',')
    parent_sql = f"""INSERT INTO {table_name} ({columns}) VALUES ({values})"""
    try:
        product_values = []
        for dict_data in list_data:
            product_values.append( (
                dict_data.get("brand_name"),
                dict_data.get("brand_url"),
                dict_data.get("car_name"),
                dict_data.get("car_url"),
                dict_data.get("status")
            ))

        try:
            batch_count = data_commit_batches_wise(connection, cursor, parent_sql, product_values)
            print(f"Parent batches executed count={batch_count}")
        except Exception as e:
            print(f"batch can not. Error : {e} ")

        cursor.close()
        connection.close()

    except Exception as e:
        ## this exception execute when error occur in try block and rollback until last save on database .
        connection.rollback()
        # print(f"Transaction failed, rolled back. Error: {e}")
        print("Transaction failed. Rolling back")
    except:
        print("except error raise ")
    finally:
        connection.close()

def fetch_cars_url_table():
    connection = get_connection()
    cursor = connection.cursor()
    query = f"SELECT * FROM {table_name} WHERE status = 'pending' ;"
 
    cursor.execute(query)
    rows = cursor.fetchall()

    result = []
    for row in rows:
        data = {
            "id": row[0],
            "brand_name": row[1],
            "brand_url": row[2],
            "car_name": row[3],
            "car_url": row[4],
            "status": row[5]
        }
        result.append(data)

    cursor.close()
    connection.close()
    return result

def update_cars_url_status(car_id, status):
    connection = get_connection()
    cursor = connection.cursor()
    sql_query = f"UPDATE {table_name} SET status = %s  WHERE id = %s ;"
    values = (status, car_id)
    cursor.execute(sql_query, values)
    connection.commit()
    cursor.close()
    connection.close()




# ------------------second table-------------
car_detail_table_name = "car_detail"

def create_car_detail_table():
    connection = get_connection()
    cursor = connection.cursor()
    try:
        query =  f"""
                CREATE TABLE IF NOT EXISTS {car_detail_table_name}(
                id INT AUTO_INCREMENT PRIMARY KEY,
                variant_name VARCHAR(255),
                variant_price VARCHAR(255),
                variant_website_url TEXT

        ); """
        cursor.execute(query)
        connection.commit()
    except Exception as e:
        print("Table creation failed")
        if connection:
            connection.rollback()
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()




def insert_car_detail_table(list_data : list):
    print("-----insert_movie_details_table--------")
    connection = get_connection()
    cursor = connection.cursor()
    dict_data = list_data[0]
    columns = ", ".join(list(dict_data.keys()))
    values = "".join([len(dict_data.keys()) * '%s,']).strip(',')
    parent_sql = f"""INSERT INTO {car_detail_table_name} ({columns}) VALUES ({values})"""
    try:
        product_values = []
        for dict_data in list_data:
            product_values.append( (
                dict_data.get("variant_name"),
                dict_data.get("variant_price"),
                dict_data.get("variant_website_url")
            ))

        try:
            batch_count = data_commit_batches_wise(connection, cursor, parent_sql, product_values)
            print(f"Parent batches executed count={batch_count}")
        except Exception as e:
            print(f"batch can not. Error : {e} ")

        cursor.close()
        connection.close()

    except Exception as e:
        ## this exception execute when error occur in try block and rollback until last save on database .
        connection.rollback()
        # print(f"Transaction failed, rolled back. Error: {e}")
        print("Transaction failed. Rolling back")
    except:
        print("except error raise ")
    finally:
        connection.close()


def fetch_car_detail_table():
    connection = get_connection()
    cursor = connection.cursor()
    query = f"SELECT * FROM {car_detail_table_name} WHERE status = 'pending' ;"
 
    cursor.execute(query)
    rows = cursor.fetchall()

    result = []
    for row in rows:
        data = {
            "id": row[0],
            "variant_name": row[1],
            "variant_price": row[2],
            "variant_website_url": row[3]
        }
        result.append(data)

    cursor.close()
    connection.close()
    return result

def update_car_detail_status(car_variant_id, status):
    connection = get_connection()
    cursor = connection.cursor()
    sql_query = f"UPDATE {car_detail_table_name} SET status = %s  WHERE id = %s ;"
    values = (status, car_variant_id)
    cursor.execute(sql_query, values)
    connection.commit()
    cursor.close()
    connection.close()





# ------------------third table-------------

features_detail_table_name = "car_features_detail"

def create_features_detail_table():
    connection = get_connection()
    cursor = connection.cursor()
    try:
        query =  f"""
                CREATE TABLE IF NOT EXISTS {features_detail_table_name}(
                id INT AUTO_INCREMENT PRIMARY KEY,
                variant_id INT,
                variant_name VARCHAR(255),
                variant_website_url TEXT,
                Highlights JSON,
                Specification JSON,
                Safety JSON, 
                Features JSON
        ); """
        cursor.execute(query)
        connection.commit()
    except Exception as e:
        print("Table creation failed")
        if connection:
            connection.rollback()
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()




def insert_features_detail_table(list_data : list, variant_id: int):
    print("-----insert_movie_details_table--------")
    connection = get_connection()
    cursor = connection.cursor()
    if not list_data:
        return
    dict_data = list_data[0]
    columns = ", ".join(list(dict_data.keys()))
    values = "".join([len(dict_data.keys()) * '%s,']).strip(',')
    parent_sql = f"""INSERT INTO {features_detail_table_name} ({columns}) VALUES ({values})"""
    try:
        product_values = []
        for dict_data in list_data:
            product_values.append( (
            dict_data.get("variant_id"),
            dict_data.get("variant_name"),
            dict_data.get("variant_website_url"),
            dict_data.get("Highlights"),
            dict_data.get("Specification"),
            dict_data.get("Safety"),
            dict_data.get("Features")
            ))
        
       
        try:
            # pass
            batch_count = data_commit_batches_wise(connection, cursor, parent_sql, product_values)
            print(f"Parent batches executed count={batch_count}")
        except Exception as e:
            # update status
            update_car_detail_status(variant_id, "failed")
            print(f"batch can not. Error : {e} ")

        cursor.close()
        connection.close()

    except Exception as e:
        ## this exception execute when error occur in try block and rollback until last save on database .
        connection.rollback()
        # print(f"Transaction failed, rolled back. Error: {e}")
        print("Transaction failed. Rolling back")
    except:
        print("except error raise ")
    finally:
        connection.close()


