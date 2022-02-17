import mysql.connector
from mysql.connector import Error
from config import db_config


# функция для соединения к базе
def create_connection_mysql_db(db_host, user_name, user_password, db_name=None):
    connection_db = None
    try:
        connection_db = mysql.connector.connect(
            host=db_host,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        print("Connection to MySQL DB successful")
    except Error as db_connection_error:
        print("Error: ", db_connection_error)
    return connection_db


connection = create_connection_mysql_db(db_config["mysql"]["host"],
                                        db_config["mysql"]["user"],
                                        db_config["mysql"]["pass"])

# курсор для взаимодействия с базой
cursor = connection.cursor()
cursor.execute("CREATE DATABASE orders")

conn = create_connection_mysql_db(db_config["mysql"]["host"],
                                  db_config["mysql"]["user"],
                                  db_config["mysql"]["pass"],
                                  "orders")

# создание таблицы
mycursor = conn.cursor()

mycursor.execute("CREATE TABLE datas (ord_no INTEGER(32), purch_amt FLOAT(32), ord_date DATE, "
                 "customer_id INTEGER(32), salesman_id INTEGER(32))")

# вставка данных в таблицу
insert_datas_table_query = "INSERT INTO datas (ord_no, purch_amt, ord_date, customer_id, salesman_id) VALUES (%s, %s, %s, %s, %s)"
orders = [("70001", "150.5", "2012-10-05", "3005", "5002"),
          ("70009", "270.65", "2012-09-10", "3001", "5005"),
          ("70002", "65.26", "2012-10-05", "3002", "5001"),
          ("70004", "110.5", "2012-08-17", "3009", "5003"),
          ("70007", "948.5", "2012-09-10", "3005", "5002"),
          ("70005", "2400.6", "2012-07-27", "3007", "5001"),
          ("70008", "5760", "2012-09-10", "3002", "5001"),
          ("70010", "1983.43", "2012-10-10", "3004", "5006"),
          ("70003", "2480.4", "2012-10-10", "3009", "5003"),
          ("70012", "250.45", "2012-06-27", "3008", "5002"),]

mycursor.executemany(insert_datas_table_query, orders)
conn.commit()


def datas_id_5002():
    """Output each order number, date and num of salesman with id 5002"""
    select_order_datas_id_5002 = "SELECT ord_no, ord_date, purch_amt FROM datas WHERE salesman_id = 5002"
    mycursor.execute(select_order_datas_id_5002)
    query_result = mycursor.fetchall()
    print("Order datas salesman with id 5002:")
    for data in query_result:
        print(data)


def salesman_id():
    """Output unique salesmen id"""
    select_salesman_id = "SELECT DISTINCT salesman_id FROM datas;"
    mycursor.execute(select_salesman_id)
    query_result_2 = mycursor.fetchall()
    print("Unique id of all salesmen:")
    for data_2 in query_result_2:
        print(data_2)


def output_filter_datas():
    """Output datas by filter order date"""
    select_filter_date_order = "SELECT * FROM datas ORDER BY ord_date"
    mycursor.execute(select_filter_date_order)
    query_result_3 = mycursor.fetchall()
    print("Filter datas from table by date order:")
    for data_3 in query_result_3:
        print(data_3)

    """Output datas by filter salesman id"""
    select_filter_salesman = "SELECT * FROM datas ORDER BY salesman_id"
    mycursor.execute(select_filter_salesman)
    query_result_4 = mycursor.fetchall()
    print("Filter datas from table by date salesman:")
    for data_4 in query_result_4:
        print(data_4)

    """Output datas by filter order number"""
    select_filter_order_num = "SELECT * FROM datas ORDER BY ord_no"
    mycursor.execute(select_filter_order_num)
    query_result_5 = mycursor.fetchall()
    print("Filter datas from table by order number:")
    for data_5 in query_result_5:
        print(data_5)

    """Output datas by filter num of order"""
    select_filter_by_num = "SELECT * FROM datas ORDER BY purch_amt"
    mycursor.execute(select_filter_by_num)
    query_result_6 = mycursor.fetchall()
    print("Filter datas from table by number:")
    for data_6 in query_result_6:
        print(data_6)


def output_datas_between_orders():
    """Output orders between 70001 and 70007"""
    select_orders_between_70001_and_70007 = "SELECT * FROM datas WHERE ord_no BETWEEN 70001 AND 70007"
    mycursor.execute(select_orders_between_70001_and_70007)
    query_result_7 = mycursor.fetchall()
    print("Orders datas between ord_no 70001 AND 70007:")
    for data_7 in query_result_7:
        print(data_7)


print(datas_id_5002())
print(salesman_id())
print(output_filter_datas())
print(output_datas_between_orders())

mycursor.close()
conn.close()





