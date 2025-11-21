import mysql.connector
from mysql.connector import Error

def can_connect(host, user, password, database):
#    "Hàm tạo kết nối với mysql"
    try :
        connection =mysql.connector.connect(host=host,
                                            user=user,password=password,
                                            database=database)
        if connection.is_connected():
            print("Kết nối thành công")
            return connection
    except Error as e:
        print(f"Lỗi kết nối: {e}")
        return None
    
# Test the function
can_connect("localhost","root","01252331055","db_etl_metadata")