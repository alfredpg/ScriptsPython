# -*- coding: utf-8 -*-

#Importación del módulo Nominatim de geopy que utilizaremos para geocodificar

#Importación del módulo csv que necesitaremos para leer y escribir en nuestros csv
import csv
#Importación del módulo shutil que usaremos para crear una copia de nuestro csv original
import shutil
import pandas as pd

#Realizamos una copia del archivo csv original que llamaremos procesado.csv
shutil.copyfile("original.csv","procesado.csv")

#Abrimos el archivo y le asignamos una variable.
archivo = open('original.csv', 'rb')

#Creamos una lista vacía donde almacenaremos las direcciones.
direcciones = []

#Utilizamos la función reader para leer el contenido del csv cuyas columnas están delimitadas por el caracter ;
csv_archivo = pd.read_csv("original.csv", delimiter=',')
#Vamos leyendo la primera columna de cada fila del csv y añadiendo la dirección a la lista.
df = pd.DataFrame({'direcc':
            ['2094 Valentine Avenue,Bronx,NY,10457',
             '1123 East Tremont Avenue,Bronx,NY,10460',
             '412 Macon Street,Brooklyn,NY,11233','Calle del Universo, 3, Valladolid',
             '302 Juan de Montoro, Aguascalientes']})

# for fila in csv_archivo.index:
#    direcciones.append(csv_archivo["direccion"][fila])


   
# =============================================================================
#  
# =============================================================================
geolocator = Nominatim(user_agent="cctmexico")

df = pd.DataFrame({'direcc':
            ['2094 Valentine Avenue,Bronx,NY,10457',
             '1123 East Tremont Avenue,Bronx,NY,10460',
             '412 Macon Street,Brooklyn,NY,11233','Calle del Universo, 3, Valladolid',
             '302 Juan de Montoro, Aguascalientes']})
    
import time
start =time.time()

from geopy.extra.rate_limiter import RateLimiter
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)
df['location'] = df['direcc'].apply(geocode)
df["coordenadas"] = df['location'].apply(lambda x: (x.latitude, x.longitude))

end = time.time()
elapsed = end-start

print(df)
print(str(elapsed),"segundos")


# =============================================================================
# 
# =============================================================================
#Cerramos el archivo abierto
archivo.close()

#Creamos un nueva lista donde se guardarán las localizaciones.
localizaciones = []

#Cada servicio de geolocalización como Google Maps, Bing Maps, Yahoo, MapQuest o Nominatim tiene su propia clase en geopy.geocoders para utilizar el servicio API.
#Creamos un objeto llamado geolocalizador a partir de la clase Nominatim().
geolocalizador = Nominatim(user_agent="my-applicati")

#Para cada dirección almacenada en la lista 'direcciones' pedimos al servicios de geocodificación de Nominatim que nos devuelva su coordenada y la guardamos en la variable 'coordenadas'. Añadimos la latitud y lo9ngitud a la lista 'localizaciones'.
for val in direcciones:
   direccion = geolocalizador.geocode([val], timeout=15)
   localizaciones.append((direccion.latitude, direccion.longitude))
   
geolocalizador.geocode()

#Creamos una cabecera para nuestro nuevo csv.
cabecera = ['DIRECCION','COORDENADAS']

#Abrimos de nuevo el archivo original y guardamos todos los datos en la variable 'datos'. Finalmente cerramos el archivo.
archivo = open('original.csv')
datos = [item for item in csv.reader(archivo, delimiter=';')]
archivo.close()

#Creamos una nueva lista llamada 'nuevos_datos'
nuevos_datos = []

#Para cada item en la lista 'datos' añadimos sus cordenadas almacenadas en la lista 'localizacion'. Finalmente incorporamos todo el conjunto a la lista 'nuevos_datos'.
for i, item in enumerate(datos):
    item.append(localizaciones[i])
    print localizaciones[i]
    nuevos_datos.append(item)

#Abrimos el archivo procesado.csv que habíamos creado y le asignamos una variable. El parámetro wb indica que queremos escribir. Añadimos una cabecera seguido de las filas con cada uno de los datos almacenados en la lista. Cerramos el archivo.
archivo = open('procesado.csv', 'wb')
csv.writer(archivo, delimiter=';', lineterminator='\n').writerow(cabecera)
csv.writer(archivo, delimiter=';', lineterminator='\n').writerows(nuevos_datos)
archivo.close()