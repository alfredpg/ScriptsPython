# import dask
import pandas as pd
# import dask.dataframe as dd
# import numpy as np
# from pandas import ExcelWriter
# from fConsolidarClientes import DistanciaCoord
# import tkinter as tk
from tkinter import filedialog
# from tkinter import messagebox
# from xlwt import Workbook
# import xlwt
import time
import sys

import warnings

#cargamos el informde de transformadores.csv
df = pd.read_csv('C:/Users/P568/Desktop/informes/InformeTranformadoresMLU.csv', encoding="ANSI", delimiter=";", on_bad_lines="skip")

#cargamos el informe historico de transf gest
df_historico = pd.read_excel('C:/Users/P568/Desktop/PROYECTOS_ISES/TRANF_GEST/20221224 TRANSF_GEST_MATRICULADOS_SIN_MATRICULAR.xlsb', 'Informe Tranformadores MLU', engine='pyxlsb')


#Eliminar duplicados equipo ruta id
df = df.drop_duplicates(subset=['Equipo Ruta Id']) 

#rellenar vacias por ceros
df.fillna(0, inplace=True)

#Eliminar ESTADO RE_PUBL Y RE_ELIM Y CEROS
indexNames = df[df['Estado'] == 'RE_ELIMIN' ].index
df.drop(indexNames , inplace=True)
indexNames = df[df['Estado'] == 'RE_PUBL' ].index
df.drop(indexNames , inplace=True)
indexNames = df[df['Estado'] == 0 ].index
df.drop(indexNames , inplace=True)

#resetamos index
df = df.reset_index()
df = df.drop(columns = 'index')

#insertamos las columnas de estado y placa
df.insert(0, "CRUCE ACT_ANT", 0)
df.insert(1, "ESTADO DEL TRANSFORMADOR 0", 0)
df.insert(2, "ESTADO DEL TRANSFORMADOR 1", 0)    
df.insert(3, "PLACA MT COLOCADA 0", 0)

#cruce anterior con el actual


#igualamos las columnas de placa
df['PLACA MT COLOCADA 0'] = df['PLACA MT COLOCADA']

#le damos el formato correcto a las coordenadas
df["Longitud"] = df["Longitud"].astype(str)
df["Latitud"] = df["Latitud"].astype(str)
df["Longitud del equipo padre"] = df["Longitud del equipo padre"].astype(str)
df["Latitud del equipo padre"] = df["Latitud del equipo padre"].astype(str)

df["Longitud"] = df.loc[:,"Longitud"].apply(str.replace,args=('.', ',')) #remplazamos
df["Latitud"] = df.loc[:,"Latitud"].apply(str.replace,args=('.', ',')) #remplazamos
df["Longitud del equipo padre"] = df.loc[:,"Longitud del equipo padre"].apply(str.replace,args=('.', ',')) #remplazamos
df["Latitud del equipo padre"] = df.loc[:,"Latitud del equipo padre"].apply(str.replace,args=('.', ',')) #remplazamos


