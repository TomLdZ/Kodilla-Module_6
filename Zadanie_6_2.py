import sqlite3
from sqlite3 import Error

class DataBase: 
   def __init__(self):
      pass

   @staticmethod
   def create_connection(db_file):
        """ create a database connection to the SQLite database
            specified by db_file
        :param db_file: database file
        :return: Connection object or None
        """
        conn = None
        try:
            conn = sqlite3.connect(db_file)
            return conn
        except Error as e:
            print(e)

        return conn

   @staticmethod
   def execute_sql(conn, sql):
        """ Execute sql
        :param conn: Connection object
        :param sql: a SQL script
        :return:
        """
        try:
            cur = conn.cursor()
            cur.execute(sql)
        except Error as e:
            print(e)

   @staticmethod
   def add_customer(conn, customer):
        """
        Create a new customer into the customer table
        :param conn:
        :param customer:
        :return: customer id
        """
        sql = '''INSERT INTO customer(first_name, last_name, email, address_id)
                    VALUES(?,?,?,?)'''
        cur = conn.cursor()
        cur.executemany(sql, customer)
        conn.commit()
        return cur.lastrowid

   @staticmethod
   def add_address(conn, address):
        """
        Create a new address into the address table
        :param conn:
        :param address:
        :return: address id
        """
        sql = '''INSERT INTO address(address, district, city_id, postal_code, phone)
                    VALUES(?,?,?,?,?)'''
        cur = conn.cursor()
        cur.executemany(sql, address)
        conn.commit()
        return cur.lastrowid

   @staticmethod
   def add_city(conn, city):
        """
        Create a new city into the city table
        :param conn:
        :param city:
        :return: city id
        """
        sql = '''INSERT INTO city(city, country_id)
                    VALUES(?,?)'''
        cur = conn.cursor()
        cur.executemany(sql, city)
        conn.commit()
        return cur.lastrowid

   @staticmethod
   def add_country(conn, country):
        """
        Create a new country into the country table
        :param conn:
        :param country:
        :return: country id
        """
        sql = '''INSERT INTO country(country)
                    VALUES(?)'''
        cur = conn.cursor()
        cur.executemany(sql, country)
        conn.commit()
        return cur.lastrowid

   @staticmethod
   def select_all(conn, table):
        """
        Query all rows in the table
        :param conn: the Connection object
        :return:
        """
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM {table}")
        rows = cur.fetchall()

        return rows

   @staticmethod
   def select_where(conn, table, **query):
        """
        Query tasks from table with data from **query dict
        :param conn: the Connection object
        :param table: table name
        :param query: dict of attributes and values
        :return:
        """
        cur = conn.cursor()
        qs = []
        values = ()
        for k, v in query.items():
            qs.append(f"{k}=?")
            values += (v,)
        q = " AND ".join(qs)
        cur.execute(f"SELECT * FROM {table} WHERE {q}", values)
        rows = cur.fetchall()
        return rows

   @staticmethod
   def update(conn, table, id, **kwargs):
        """
        update status, begin_date, and end date of a task
        :param conn:
        :param table: table name
        :param id: row id
        :return:
        """
        parameters = [f"{k} = ?" for k in kwargs]
        parameters = ", ".join(parameters)
        values = tuple(v for v in kwargs.values())
        values += (id, )

        sql = f''' UPDATE {table}
                    SET {parameters}
                    WHERE {table}_id = ?'''
        try:
            cur = conn.cursor()
            cur.execute(sql, values)
            conn.commit()
            print("OK")
        except sqlite3.OperationalError as e:
            print(e)

   @staticmethod
   def delete_where(conn, table, **kwargs):
        """
        Delete from table where attributes from
        :param conn:  Connection to the SQLite database
        :param table: table name
        :param kwargs: dict of attributes and values
        :return:
        """
        qs = []
        values = tuple()
        for k, v in kwargs.items():
            qs.append(f"{k}=?")
            values += (v,)
        q = " AND ".join(qs)

        sql = f'DELETE FROM {table} WHERE {q}'
        cur = conn.cursor()
        cur.execute(sql, values)
        conn.commit()
        print("Deleted")

   @staticmethod
   def delete_all(conn, table):
        """
        Delete all rows from table
        :param conn: Connection to the SQLite database
        :param table: table name
        :return:
        """
        sql = f'DELETE FROM {table}'
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        print("Deleted")

if __name__ == "__main__":

    create_customer_sql = """
    -- customer table
    CREATE TABLE IF NOT EXISTS customer (
        customer_id integer PRIMARY KEY,
        first_name VARCHAR(250) NOT NULL,
        last_name VARCHAR(250) NOT NULL,
        email VARCHAR(250),
        address_id integer NOT NULL,
        FOREIGN KEY (address_id) REFERENCES address (id)
    );
    """

    create_address_sql = """
    -- adress table
    CREATE TABLE IF NOT EXISTS address (
        address_id integer PRIMARY KEY,
        address VARCHAR(250) NOT NULL,
        district VARCHAR(250) NOT NULL,
        city_id integer NOT NULL,
        postal_code VARCHAR(250),
        phone VARCHAR(250) NOT NULL,
        FOREIGN KEY (city_id) REFERENCES city (id)
    );
    """

    create_city_sql = """
    -- city table 
    CREATE TABLE IF NOT EXISTS city (
        city_id integer PRIMARY KEY,
        city VARCHAR(250) NOT NULL,
        country_id integer NOT NULL,
        FOREIGN KEY (country_id) REFERENCES country (id)
    );
    """

    create_country_sql = """
    -- country table 
    CREATE TABLE IF NOT EXISTS country (
        country_id integer PRIMARY KEY,
        country VARCHAR(250) NOT NULL
    );
    """

    customers = [("Mary", "Smith", "mary.smith@gmail.com", 1),
                 ("Patricia", "Johnson",	"patricia.johnson@gmail.com", 2),
                 ("Linda", "Williams", "linda.williams@gmail.com", 3),
                 ("Barbara", "Jones", "barbara.jones@sgmail.com", 4),
                 ("Elizabeth", "Brown", "elizabeth.brown@gmail.com", 5)]

    addresses = [("1913 Hanoi Way", "Nagasaki", 1, "35200", "28303384290"),
                 ("1121 Loja Avenue",	 "California", 2, "17886", "838635286649"),
                 ("692 Joliet Street", "Attika", 3, "83579", "448477190408"),
                 ("1566 Inegl Manor", "Mandalay", 4, "53561", "705814003527"),
                 ("53 Idfu Parkway", "Nantou", 5, "42399", "10655648674")]
 
    cities = [("Sasebo", 1),
              ("San Bernardino", 2),
              ("Athenai", 3),
              ("Myingyan", 4),
              ("Nantou", 5)]
    
    countries = [("Japan",), ("United States",), ("Greece",), ("Myanmar",), ("Taiwan",)]
    

    db_file = "database.db"

    conn = DataBase.create_connection(db_file)

    if conn is not None:

        DataBase.execute_sql(conn, create_country_sql)
        DataBase.execute_sql(conn, create_city_sql)
        DataBase.execute_sql(conn, create_address_sql)
        DataBase.execute_sql(conn, create_customer_sql)
       
        
        DataBase.add_customer(conn, customers)
        DataBase.add_address(conn, addresses)
        DataBase.add_city(conn, cities)
        DataBase.add_country(conn, countries)
        

        for row in DataBase.select_all(conn, "customer"):
            print(row)
        
        for row in DataBase.select_all(conn, "address"):
            print(row)
        
        for row in DataBase.select_all(conn, "city"):
            print(row)
        
        for row in DataBase.select_all(conn, "country"):
            print(row)
        
        print(DataBase.select_where(conn, "city", city_id=2))

        DataBase.update(conn, "customer", 1, first_name="Maria") 

        print(DataBase.select_where(conn, "customer", customer_id=1))

        DataBase.delete_where(conn, "customer", customer_id=5)

        for row in DataBase.select_all(conn, "customer"):
             print(row)

        DataBase.delete_all(conn, "customer")

        for row in DataBase.select_all(conn, "customer"):
             print(row)

        