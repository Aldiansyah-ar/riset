import pandas as pd
from datetime import datetime
import psycopg2
from psycopg2 import sql

def create_table(table_name):
    with conn.cursor() as cursor:
        create_table_query = sql.SQL(
            """
            CREATE TABLE IF NOT EXISTS {table_name} (
                id SERIAL PRIMARY KEY,
                source VARCHAR(100),
                title TEXT,
                url TEXT,
                content TEXT,
                date VARCHAR(100),
                parsed_date TIMESTAMP
            )
            """
        ).format(table_name=sql.Identifier(table_name))

        cursor.execute(create_table_query)
        conn.commit()

def insert_data(table_name):
    create_table(table_name)
    data = data_extend(table_name)
    excluded_fields = ['id']
    # Iterasi per baris pada DataFrame
    with conn.cursor() as cursor:
        for _, row in data.iterrows():
            # Filter kolom berdasarkan excluded_fields
            filtered_row = {k: v for k, v in row.to_dict().items() if k not in excluded_fields}
            
            # Siapkan kolom dan nilai untuk query
            columns = ', '.join([f'"{column}"' for column in filtered_row.keys()])
            placeholders = ', '.join(['%s'] * len(filtered_row))
            values = list(filtered_row.values())
            
            # Buat query SQL untuk insert
            query = f"""
                INSERT INTO {table_name} ({columns})
                VALUES ({placeholders})
            """
            # Eksekusi query untuk baris saat ini
            cursor.execute(query, values)

def parse_date(date):
    month_dict = {
        'Jan': '01',
        'Feb': '02',
        'Mar': '03',
        'Apr': '04',
        'Mei': '05',
        'Jun': '06',
        'Jul': '07',
        'Agu': '08',
        'Sep': '09',
        'Okt': '10',
        'Nov': '11',
        'Des': '12'
    }

    date = date.replace(' WIB','').replace('  202', ' 202')
    date = date.split(', ')[1]
    date_split = date.split(' ')
    day = date_split[0]
    month = month_dict[date_split[1]]
    year = date_split[2]
    hour = date_split[3]
    date = year + '-' + month + '-' + day + ' ' + hour
    date = datetime.strptime(date, "%Y-%m-%d %H:%M")
    date = datetime.strftime(date, "%Y-%m-%d %H:%M:%S")
    return date

def data_extend(table_name):
    df = pd.read_csv(f'./data_scraping/{table_name}.csv')
    df['parsed_date'] = df['date'].apply(parse_date)
    return df

conn = psycopg2.connect(
    dbname="news",
    user="postgres",
    password="admin",
    host="localhost",
    port="5432"
)

conn.commit()
conn.close()