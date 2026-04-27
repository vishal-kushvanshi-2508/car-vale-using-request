# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import mysql.connector
from mysql.connector import Error

class CarValeScrapyPipeline:

    def __init__(self):
        # Database configuration
        self.host = "localhost"
        self.user = "root"
        self.password = "actowiz"  # replace with your MySQL password
        self.port = "3306"
        self.database = "car_vale_scrapy_db"

    def open_spider(self, spider):
        """Runs when spider starts"""
        try:
            # Connect to MySQL server
            self.conn = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                port=self.port
            )
            self.cursor = self.conn.cursor()

            # Create database if not exists
            self.cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.database}")
            self.conn.database = self.database


            # ================================
            #  1. Create all_category table
            # ================================
            self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS cars_url (
                id INT AUTO_INCREMENT PRIMARY KEY,
                brand_name VARCHAR(200),
                brand_url TEXT,
                car_name VARCHAR(200),
                car_url TEXT,
                status VARCHAR(50) DEFAULT 'pending'
            )
            """)

            # # # ==================================
            # # # 2. Create product_api table
            # # # ==================================
            self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS variant_details (
                id INT AUTO_INCREMENT PRIMARY KEY,
                car_url TEXT,
                variant_name VARCHAR(200),
                variant_price VARCHAR(200),
                variant_website_url TEXT
            )
            """)

            # # # 3. Create product_api table
            # # # ==================================
            self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS car_features_detail (                                
                id INT AUTO_INCREMENT PRIMARY KEY,
                variant_id INT,
                variant_name VARCHAR(255),
                variant_website_url TEXT,
                Highlights JSON,
                Specification JSON,
                Safety JSON, 
                Features JSON
            )
            """)

            self.conn.commit()
        except Error as e:
            spider.logger.error(f"Error connecting to MySQL: {e}")


    def process_item(self, item, spider):
        # print("---process_item---", item)

        # -------------------------------
        # Insert into all_category
        # -------------------------------
        if item.get("type") == "car_urls":
            query = """
            INSERT INTO cars_url (brand_name, brand_url, car_name, car_url, status)
            VALUES (%s, %s, %s, %s, %s)
            """

            values = (
                item.get("brand_name"),
                item.get("brand_url"),
                item.get("car_name"),
                item.get("car_url"),
                item.get("status", "pending")
            )

            self.cursor.execute(query, values)
            self.conn.commit()

        # -------------------------------
        #  Insert into product_api
        # -------------------------------
        elif item.get("type") == "variant_details":
            query = """
            INSERT INTO variant_details (car_url, variant_name, variant_price, variant_website_url)
            VALUES (%s, %s, %s, %s)
            """

            values = (
                item.get("car_url"),
                item.get("variant_name"),
                item.get("variant_price"),
                item.get("variant_website_url")
            )

            self.cursor.execute(query, values)
            self.conn.commit()


        # -------------------------------
        #  Insert into product_api
        # -------------------------------
        elif item.get("type") == "car_features_detail":
            # print("data inter is now ")
            query = """
            INSERT INTO car_features_detail (variant_id, variant_name, variant_website_url, Highlights, Specification, Safety, Features)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
            """

            values = (
                item.get("variant_id"),
                item.get("variant_name"),
                item.get("variant_website_url"),
                item.get("Highlights"),
                item.get("Specification"),
                item.get("Safety"),
                item.get("Features")
            )

            self.cursor.execute(query, values)
            self.conn.commit()

        return item





    # ====================================
    #  Close Connection
    # ====================================
    def close_spider(self, spider):
        self.cursor.close()
        self.conn.close()