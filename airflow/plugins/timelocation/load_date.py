import pandas as pd
import psycopg2 

def readTimeExcel():
        time_df = pd.read_excel('./data/SampleDateDim.xls', sheet_name='LoadDates')
        
        print('==========================')
        print(time_df.columns)
        print(time_df.head())
        print('==========================')
        # Connect to PostgreSQL
        conn = psycopg2.connect(
                database="LdtbxhStage",
                user="postgres",
                password="nhanbui",
                host="host.docker.internal",
                port="5434"
        )
        cur = conn.cursor()
        # Insert data into PostgreSQL
        print('start to insert...')
        for row in time_df.to_numpy():
                cols = ','.join(list(time_df.columns))
                # print(type(row[1]))
                for idx, val in enumerate(row):
                        if isinstance(val, pd.Timestamp):
                                row[idx] = str(val)
                cur.execute(f'''INSERT INTO "stgDate"({cols}) VALUES {tuple(row)}''')
        print('inserted sucessfully!')
        conn.commit()
        cur.close()
        conn.close()