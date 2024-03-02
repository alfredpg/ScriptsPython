# =============================================================================
# LIBRERIAS
# =============================================================================
# import dask
import pandas as pd
from openpyxl import load_workbook
# import dask.dataframe as dd
# import numpy as np
# from pandas import ExcelWriter
# from fConsolidarClientes import DistanciaCoord
import tkinter as tk
from tkinter import filedialog
# from tkinter import messagebox
# from xlwt import Workbook
# import xlwt
import time
import sys
import warnings

# =============================================================================
# FUNCIONES
# =============================================================================

# =============================================================================
# CONSTANTES
# =============================================================================

nombreExcel = "MAESTRO_VIABILIDADES.xlsx"
numero_iteraciones = 10

# =============================================================================
# Colecciones 
# =============================================================================
lista_df_temp = []

# =============================================================================
# Cargue de BBDD
# =============================================================================
file_path = filedialog.askopenfilename(defaultextension=".xls")
if file_path is None:
    # quit()
    sys.exit()
df = pd.read_excel(file_path, header=3)
#df = pd.read_excel(nombreExcel, header=3)
df = df.drop(columns = 'Unnamed: 0')
print("Archivo cargado correctamente con "+str(len(df))+" filas \n")

# =============================================================================
# MAIN
# =============================================================================
warnings.filterwarnings('ignore')
warnings.warn('DelftStack')
warnings.warn('Do not show this message')

primeras_columnas = df.iloc[:, :16]
ultimas_columnas = df.iloc[:, -6:]
df_general = pd.concat([primeras_columnas, ultimas_columnas], axis=1)

numColumnasInicio = 16
numColumnasFin = 31
for i in range(10):
    print("cargando cable operadores "+str(i+1))
    df_id = df.iloc[:,1]
    df_cableOperadores = df.iloc[:,numColumnasInicio:numColumnasFin]
    df_temp = pd.concat([df_general, df_cableOperadores], axis=1)
    
    numColumnasInicio = numColumnasFin
    numColumnasFin = numColumnasInicio+15
    
    lista_df_temp.append(df_temp)

for i in range(10):
    lista_df_temp[i].columns = lista_df_temp[0].columns        

df_final = pd.concat(lista_df_temp, axis=0, ignore_index=True)
df_final["Cableoperador 1"].fillna('<Null>', inplace=True)
print("\ncantidad conv1 : "+str(len(df_final)))
df_finalNull = df_final[df_final["Cableoperador 1"] == "<Null>"]
df_final.drop(df_finalNull.index , inplace=True)
print("se eliminaron "+str(len(df_finalNull))+ " Null nan")
print("OK conv1 : "+str(len(df_final)))

id_vars = df_final.iloc[:, :24].columns
value_vars = df_final.iloc[:,24:-1].columns

df_melted = df_final.melt(id_vars=id_vars, value_vars=value_vars, var_name='tipo_elemento', value_name='cantidad')
print("\ncantidad conv2 : "+str(len(df_melted)))
df_melted0 = df_melted[df_melted["cantidad"] == 0]  
df_melted.drop(df_melted0.index , inplace=True)
print("se eliminaron "+str(len(df_melted0))+ " ceros (0)")
print("OK conv2 : "+str(len(df_melted)))

root = tk.Tk()
root.withdraw() 

nombre_archivo = filedialog.asksaveasfilename(defaultextension=".xls", filetypes=[("Excel Files", "*.xlsx")])

if nombre_archivo:
    print("Gardando archivo ..........")
    df_melted.to_excel(nombre_archivo, index=False)
    print("DataFrame guardado exitosamente como:", nombre_archivo)
























