import googlemaps
from datetime import datetime
import pandas as pd
from geopy.extra.rate_limiter import RateLimiter

gmaps = googlemaps.Client(key='AIzaSyD03LjLVQqAFqU-GBzhCoEDnkjQY8fReTc')

# Geocoding an address
#Cl. 23 #15 09, Santa Marta, Magdalena, Colombia
lista_Errores =[]

df = pd.read_excel('Análisis SBN Los Rosales & Cordoba.xlsx',  sheet_name='Sum_SBN')


# geocode = RateLimiter(gmaps.geocode, min_delay_seconds=1)
# df['location'] = df['direccion'].apply(geocode.)
# df["coordenadas"] = df['location'].apply(lambda x: (x.latitude, x.longitude))
print(len(df))

for i in df.index:
    try:
        consulta = gmaps.geocode(str(df["DIRECC"][i]))
        detalles = consulta[0]
        formatNombre = detalles["formatted_address"]
        geometry = detalles["geometry"]
        location = geometry["location"]
        lat = location["lat"]
        lng = location["lng"]
        
        df["Latitud API"][i] = str(lat)
        df["Longitud API"][i] = str(lng)
        df["Direcc Format"][i] = str(formatNombre)
           
        print(i,"/",len(df))
    except Exception as e:
        print("ERROR EN LA DIRECCION : "+str(df["PRODUCTO HIJO"][i]))
        lista_Errores.append(df["PRODUCTO HIJO"][i])

df.to_excel('resultado_Analisis.xlsx', index = False)
    

# geocode_result = gmaps.geocode('Cra. 18 #21-13, Maicao, La Guajira, Colombia')

# aa = geocode_result[0]

# bb = aa["geometry"]

# cc = bb["location"]

# dd = {str(cc["lat"])+" "+str(cc["lng"])}

# print(dd)


# # Look up an address with reverse geocoding
# reverse_geocode_result = gmaps.reverse_geocode((40.714224, -73.961452))

# # Request directions via public transit
# now = datetime.now()
# directions_result = gmaps.directions("Sydney Town Hall",
#                                      "Parramatta, NSW",
#                                      mode="transit",
#                                      departure_time=now)

# # Validate an address with address validation
# addressvalidation_result =  gmaps.addressvalidation(['1600 Amphitheatre Pk'], 
#                                                     regionCode='US',
#                                                     locality='Mountain View', 
#                                                     enableUspsCass=True)

