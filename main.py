from flask import Flask, jsonify,request
from io import BytesIO
import pandas as pd
import sqlite3


inerg = Flask(__name__)

def connect_db():
    return sqlite3.connect('inerg.db')
print("---connected to db ---")

def create_table():
    conn = connect_db()
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS annual_data_table (
            well_number TEXT PRIMARY KEY,
            oil INTEGER,
            gas INTEGER,
            brine INTEGER
        )
    ''')
    conn.commit()
    conn.close()
create_table()
print("--table creation success---")


@inerg.route('/welcome')
def check():
    return 'Hello, Inerg!'
#calculate the data and save to db
@inerg.route('/calculate/annual_data',methods=['GET'])
def annual_calculations():
    try:
        file_path = 'static/20210309_2020_1 - 4.xls'
        data = pd.read_excel(file_path, engine='xlrd')
        print(data.head())
        print(data.columns)
        annual_data = {}
        for index, row in data.iterrows():
            well_num = row['API WELL  NUMBER']
            if well_num in annual_data:
                annual_data[well_num]['OIL'] += row['OIL']
                annual_data[well_num]['GAS'] += row['GAS']
                annual_data[well_num]['BRINE'] += row['BRINE']
            else:
                annual_data[well_num] = {
                    'OIL': row['OIL'],
                    'GAS': row['GAS'],
                    'BRINE': row['BRINE']
                }
        print(annual_data)
        conn = connect_db()
        cursor = conn.cursor()
        for well_num, prod in annual_data.items():
            cursor.execute("INSERT OR REPLACE INTO annual_data_table (well_number, oil, gas, brine) VALUES (?, ?, ?, ?)", (well_num, prod['OIL'], prod['GAS'], prod['BRINE']))
        conn.commit()
        conn.close()
        response={}
        response['status'] = True
        response['status_code'] = 200
        response['data'] = []
        response['description'] = "success"
        return jsonify(response)       
    except Exception as e:
        response={}
        response['status'] = False
        response['status_code'] = 500
        response['data'] = []
        response['description'] = str(e)
        return jsonify(response)

#fetch the data
@inerg.route('/data', methods=['GET'])
def get_annual_data():
    try:
        well_number = request.args.get('well') 
        if not well_number:
            response={}
            response['status'] = False
            response['status_code'] = 400
            response['data'] = []
            response['description'] = "Please provide well number"
            return jsonify(response)
        conn = connect_db()
        cursor = conn.cursor()
        cursor.execute("SELECT oil, gas, brine FROM annual_data_table WHERE well_number = ?", (well_number,))
        row = cursor.fetchone()
        conn.close()
        print(row)
        if row:
            data = {
                'oil': row[0],
                'gas': row[1],
                'brine': row[2]
            }
            response={}
            response['status'] = True
            response['status_code'] = 200
            response['data'] = data
            response['description'] = "success"
            return jsonify(response)
        else:
            response={}
            response['status'] = False
            response['status_code'] = 404
            response['data'] = []
            response['description'] = "Data not found"
            return jsonify(response)
    except Exception as e:
        response={}
        response['status'] = False
        response['status_code'] = 500
        response['data'] = []
        response['description'] = str(e)
        return jsonify(response)



if __name__ == "__main__":
    inerg.run(debug=True, port=8080)