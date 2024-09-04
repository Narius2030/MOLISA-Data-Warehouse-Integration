from faker import Faker
import random
from datetime import datetime
import json
import psycopg2
from providers import CustomProviderFaker



class GenerateSamplesNCC():
    def __init__(self, config, locale='vi_VN') -> None:
        self.config = config
        self.cstprovider = CustomProviderFaker()
        self.fake = Faker(locale=locale)
        # add customized provider
        self.fake.add_provider(self.cstprovider.nations_provider)
        self.fake.add_provider(self.cstprovider.district_codes_provider)

    def generate_cmnd_number(self):
        return ''.join(str(random.randint(0, 9)) for _ in range(12))

    def generate_ncc_category(self, profile_code):
        cates = ""
        for c in profile_code:
            if c.isnumeric():
                break
            cates += c
        return cates

    def fake_ncc_profile(self, num_rows=10):
        data = []
        for _ in range(num_rows):
            profile_code = f'AHLD{random.randint(100, 999)}'
            data.append((
                profile_code,  # Profile code
                self.generate_ncc_category(profile_code),  # Contributor category code
                self.fake.name(),  # Họ tên
                self.fake.date_of_birth(minimum_age=40, maximum_age=80),  # Date of birth
                random.choice([True, False]),  # Sex
                self.fake.nations(),  # Ethnic
                self.generate_cmnd_number(),  # Identity card
                '67',  # Province code
                self.fake.district_codes(),  # District code
                self.fake.date_between_dates(datetime(2022,1,1), datetime(2022,12,31)),  # Decide monthly date
                self.fake.date_between_dates(datetime(2022,1,1), datetime(2022,12,31)),  # Decide yearly date
                f'{random.randint(100000, 999999)}',  # Decide monthly number
                f'{random.randint(100000, 999999)}',  # Decide yearly number
                self.fake.date_between(datetime(2022,1,1), datetime(2022,12,31)), # Date of subsidy spending,
                random.choice([True, False]),
                datetime.now()
            ))
        return data

    def insert_postgres(self, insert_stmt, values):
        with psycopg2.connect(
            database="ncc",
            user=f"{self.config['USER']}",
            password=f"{self.config['PASSWORD']}",
            host=f"{self.config['HOST_LOCAL']}",
            port=f"{self.config['PORT']}"
        ) as conn:
            with conn.cursor() as cur:
                for value in values:
                    try:
                        cur.execute(insert_stmt, value)
                    except Exception as exc:
                        cur.execute("ROLLBACK")
                        print(str(exc))


if __name__=='__main__':
    with open("./airflow/config.json", "r") as file:
            config = json.load(file)
            
    generator = GenerateSamplesNCC(config=config)
    
    # generate samples
    fake_data = generator.fake_ncc_profile(10)
    print(fake_data[:5])
            
    # # insert to postgresql
    # insert_stmt = '''INSERT INTO profile."nccProfile"(profile_code, ncc_code, full_name, 
    #                             birth_of_date, sex, ethnic, identity_number, 
    #                             province_code, district_code, decided_monthly_date, decided_once_date, 
    #                             decided_monthly_num, decided_once_num, start_subsidize, support_bhyt, created_date)
    #                 VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
    # generator.insert_postgres(config, insert_stmt, fake_data)
    