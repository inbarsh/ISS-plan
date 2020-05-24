import json
import mysql.connector


class SQLHandler:
    def __init__(self, name):
        sql_config = open("config/sql_config.json")
        sql_config = json.load(sql_config)

        self.user = sql_config["user"]
        self.password = sql_config["password"]
        self.host = sql_config["host"]
        self.database = sql_config["database"]
        self.mydb = None
        self.mycursor = None
        self.name = name

    def connect_db(self):
        try:
            global mydb
            mydb = mysql.connector.connect(user=self.user, password=self.password, host=self.host, database=self.database)

            global mycursor
            mycursor = mydb.cursor()

        except Exception as exc:
            print(exc)
            return False
        return True

    @staticmethod
    def create_table(query):
        try:
            mycursor.execute(query)

        except Exception as exc:
            if "already exists" not in exc.msg:
                print(exc)
                return False
        return True

    def create_orbital_table(self):
        self.create_table("create table orbital_data_" + self.name + "(city varchar(250),timestamp datetime, duration_seconds integer(10))")

    def create_city_stats_table(self):
        self.create_table("create table city_stats_" + self.name + "(city varchar(250), avg_daily_flights_amount double)")

    def reset_city_stats_table(self, data):
        try:
            self.create_city_stats_table()
            self.insert_to_table("city_stats_" + self.name + "(city,avg_daily_flights_amount)", "(%s,%s)", data)

        except Exception as exc:
            if "already exists" not in exc.msg:
                print(exc)
                return False
        return True

    @staticmethod
    def insert_to_table(table_name_and_columns, values, data):
        query = "INSERT INTO " + table_name_and_columns + " VALUES " + values
        try:
            mycursor.executemany(query, data)
            mydb.commit()

        except Exception as exc:
            print(exc)
            return False
        return True

    def create_count_avg_sp(self):
        return self.create_stored_procedure("count_avg_daily_flights_amount", "CREATE PROCEDURE count_avg_daily_flights_amount() BEGIN " \
        "CREATE TABLE tmp ( " \
        "city varchar(255), " \
        "avg_daily_flights_amount double" \
        ");" \
        "insert into tmp " \
        "select city ,avg(avg_daily_flights_amount) as avg_daily_flights_amount " \
        "from (select orbital_data_" + self.name + ".city,count(orbital_data_" + self.name + ".city) " \
        "as avg_daily_flights_amount , cast(orbital_data_" + self.name + ".timestamp as date) as dt " \
        "from orbital_data_" + self.name + " group by cast(orbital_data_" + self.name + ".timestamp as date), " \
        "orbital_data_" + self.name + ".city order by orbital_data_" + self.name + ".city asc) as tmp_tbl  group by city;" \
        "update city_stats_" + self.name + " csl," \
        "tmp " \
        "SET " \
        "csl.avg_daily_flights_amount = tmp.avg_daily_flights_amount " \
        "WHERE " \
        "csl.city= tmp.city;" \
        "" \
        "DROP TABLE tmp;" \
        "end;")

    @staticmethod
    def create_stored_procedure(sp_name, body):
        try:
            mycursor.execute(body)
            return sp_name
        except Exception as exc:
            if "already exists" not in exc.msg:
                print(exc)
                return False
        return True

    @staticmethod
    def run_stored_procedure(sp_name):
        try:
            mycursor.execute("call " + sp_name + "();")
            return True
        except Exception as exc:
            print(exc)
            return False

    def combine_data(self):
        try:
            query = "CREATE TABLE union_table (" \
                        "city varchar(255)," \
                        "population int," \
                        "max_temperature int," \
                        "min_temperature int," \
                        "update_date DATETIME," \
                        "avg_daily_flights_amount double" \
                        ");" \
                        " INSERT INTO union_table (city,population,max_temperature,min_temperature,update_date)" \
                        " SELECT * FROM (SELECT * FROM(SELECT * FROM(SELECT * FROM city_stats_beer_sheva csbs " \
                        " union all  select * from city_stats_eilat cse ) as eilat" \
                        " union all select * FROM city_stats_haifa csh) as haifa" \
                        " union all select * from city_stats_tel_aviv ) as tlv;" \
                        " update union_table ut,city_stats_" + self.name + " csl SET ut.avg_daily_flights_amount =  csl.avg_daily_flights_amount WHERE csl.city= ut.city;"
            for result in mycursor.execute(query, multi=True):
                pass
        except Exception as exc:
            print(exc)
            return False
        return True

    @staticmethod
    def get_data_from_table(table_name):
        try:
            mycursor.execute("select * from " + table_name + ";")
            return mycursor.fetchall()
        except Exception as exc:
            print(exc)
            return None

    @staticmethod
    def get_col_names(table_name):
        try:
            mycursor.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'" + table_name + "';")
            return [column[0] for column in mycursor.fetchall()]
        except Exception as exc:
            print(exc)
            return None

    @staticmethod
    def delete_table(table_name):
        try:
            mycursor.execute("drop table " + table_name + ";")
        except Exception as exc:
            print(exc)
            return False
        return True
