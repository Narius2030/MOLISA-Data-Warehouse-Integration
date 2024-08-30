from faker import Faker
import random
from datetime import datetime
import json
import psycopg2


fake = Faker(locale='vi_VN')
fake.add_provider(nations_provider)
fake.add_provider(district_codes_provider)


def generate_cmnd_number():
    return ''.join(str(random.randint(0, 9)) for _ in range(12))


def generate_ncc_category():
    data = {
        'LS': 'Liệt sĩ',
        'BMVNAH': 'Bà mẹ Việt Nam anh hùng',
        'AHLLVTND': 'Anh hùng lực lượng vũ trang nhân dân',
        'AHLD': 'Anh hùng lao động'
    }
    key = random.choice(list(data.keys()))
    return key, data[key]


def generate_fake_data(num_rows=10):
    data = []
    for _ in range(num_rows):
        data.append((
            f'AHLD{random.randint(100, 999)}',  # Profile code
            generate_ncc_category()[0],  # Contributor category code
            fake.name(),  # Họ tên
            fake.date_of_birth(minimum_age=40, maximum_age=80),  # Date of birth
            random.choice([True, False]),  # Sex
            fake.nations(),  # Ethnic
            generate_cmnd_number(),  # Identity card
            '67',  # Province code
            fake.district_codes(),  # District code
            fake.date_between_dates(datetime(2022,1,1), datetime(2022,12,31)),  # Decide monthly date
            fake.date_between_dates(datetime(2022,1,1), datetime(2022,12,31)),  # Decide yearly date
            f'{random.randint(100000, 999999)}',  # Decide monthly number
            f'{random.randint(100000, 999999)}',  # Decide yearly number
            fake.date_between(datetime(2022,1,1), datetime(2022,12,31)), # Date of subsidy spending,
            random.choice([True, False]),
            datetime.now()
        ))
    return data


def insert_postgres(config, insert_stmt, values):
    with psycopg2.connect(
        database="ncc",
        user=f"{config['USER']}",
        password=f"{config['PASSWORD']}",
        host=f"{config['HOST_LOCAL']}",
        port=f"{config['PORT']}"
    ) as conn:
        with conn.cursor() as cur:
            for value in values:
                try:
                    cur.execute(insert_stmt, value)
                except Exception as exc:
                    cur.execute("ROLLBACK")
                    print(str(exc))


if __name__=='__main__':

    # Ví dụ sử dụng:
    fake_data = generate_fake_data(10)
    # print(fake_data[:]) # In 5 dòng dữ liệu đầu tiên

    with open("./airflow/config.json", "r") as file:
            config = json.load(file)
            
    insert_stmt = '''INSERT INTO profile."nccProfile"(profile_code, ncc_code, full_name, 
                                birth_of_date, sex, ethnic, identity_number, 
                                province_code, district_code, decided_monthly_date, decided_once_date, 
                                decided_monthly_num, decided_once_num, start_subsidize, support_bhyt, created_date)
                    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
    insert_postgres(config, insert_stmt, fake_data)