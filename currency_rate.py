import requests
import csv
import psycopg2
import pandas as pd

conn = psycopg2.connect(database="postgres",
                            user='username', password='123456', 
                            host='127.0.0.1', port='5432'
    )
conn.autocommit = True

def rate_API():
  url = 'https://api.exchangerate.host/timeseries?start_date=2023-07-01&end_date=2023-07-31&base=BTC&symbols=BTC,RUB'
  response = requests.get(url)

  data = response.json()
  rates = data['rates']
  return rates

def create_csv():
    csv_filename = 'exchange_rates.csv'
    with open("D:/csv/exchange_rates.csv", "w", newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(['Date', 'BTC', 'RUB'])

            # Записываем данные в CSV файл
        for date, details in rate_API().items():
            csv_writer.writerow([date, details['BTC'], details['RUB']])

    with open('D:/csv/exchange_rates.csv', 'r') as csv_file:
            csv_reader = csv.reader(csv_file)
            
            next(csv_reader)
            
            data = [row for row in csv_reader]
            return data

def create_load_db():

    cursor = conn.cursor()
    
    
    sql = '''CREATE TABLE IF NOT EXISTS RATE(id serial NOT NULL, data DATE, BTC decimal(10, 2), RUB decimal(10, 2));'''

    cursor.execute(sql) 

    for i in create_csv():
        sql2 = '''INSERT INTO rate(data, BTC, RUB) VALUES(%s, %s, %s);'''

        cursor.execute(sql2, (i[0], i[1], i[2]))

def calc_load():
    
    sql3 = '''SELECT * FROM rate'''

    data_sql = pd.read_sql_query(sql3, conn)

    min_rate = data_sql['rub'].min()
    max_rate = data_sql['rub'].max()
    mean_rate = data_sql['rub'].mean()

    max_rate_index = data_sql['rub'].idxmax()
    max_rate_date = data_sql.loc[max_rate_index, 'data']

    min_rate_index = data_sql['rub'].idxmin()
    min_rate_date = data_sql.loc[min_rate_index, 'data']

    last_data_rate = data_sql['rub'].tail(1).iloc[0]

    rate = 'BTC к RUB'
    month = '2023-07'
    
    cursor = conn.cursor()
    
    sql4 = '''CREATE TABLE IF NOT EXISTS calculation(max_rate_date DATE, min_rate_date DATE, max_rate decimal(10, 2), min_rate decimal(10, 2), mean_rate decimal(10, 2), last_data_rate decimal(10, 2), rate varchar(15), month varchar(15));'''

    cursor.execute(sql4)

    sql5 ='''INSERT INTO calculation values(%s, %s, %s, %s, %s, %s, %s, %s)'''
    
    cursor.execute(sql5, (max_rate_date, min_rate_date, max_rate, min_rate, mean_rate, last_data_rate, rate, month))

create_load_db()
calc_load()