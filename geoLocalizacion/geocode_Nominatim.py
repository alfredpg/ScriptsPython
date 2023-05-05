import pandas as pd
import time
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

geolocator = Nominatim(user_agent="cctmexico")

# df = pd.DataFrame({'direcc':
#             ['2094 Valentine Avenue,Bronx,NY,10457',
#              '1123 East Tremont Avenue,Bronx,NY,10460',
#              '412 Macon Street,Brooklyn,NY,11233','Calle del Universo, 3, Valladolid',
#              '302 Juan de Montoro, Aguascalientes']})
    
# df = pd.DataFrame({'direcc':
#             ['CL. 63 Cra. 14C 52,SOLEDAD,ATLANTICO,COLOMBIA',
#             'Cl. 23 #15-09, Santa Marta, Magdalena, Colombia',
#             'Cra. 19b Cra. 23-36, Santa Marta, Magdalena, Colombia',
#              '1123 East Tremont Avenue,Bronx,NY,10460']})
df = pd.DataFrame({'direcc':
            ['street= 15-09 23, Santa Marta, Magdalena, Colombia',
             '1123 East Tremont Avenue,Bronx,NY,10460']})

start =time.time()

geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)
df['location'] = df['direcc'].apply(geocode)
df["coordenadas"] = df['location'].apply(lambda x: (x.latitude, x.longitude))

end = time.time()
elapsed = end-start

print(df)
print(str(elapsed),"segundos")
