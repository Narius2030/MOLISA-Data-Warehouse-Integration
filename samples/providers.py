from faker.providers import DynamicProvider

class CustomProviderFaker(DynamicProvider):
    def __init__(self) -> None:
        self.__nations_provider = DynamicProvider(
            provider_name="nations",
            elements=["Kinh",  "M'Nông", "Tày", "Thái", "E Đê", "Nùng", "Cơ Tu", "Tà Ôi", "Phù Lá"]
        )
        self.__district_codes_provider = DynamicProvider(
            provider_name="district_codes",
            elements=["660", "661", "662", "663", "664", "665", "666", "667"]
        )
        
    @property
    def nations_provider(self):
        return self.__nations_provider
    
    @property
    def district_codes_provider(self):
        return self.__district_codes_provider
    
    
if __name__=='__main__':
    cstprovider = CustomProviderFaker()
    print(cstprovider.district_codes_provider)
        