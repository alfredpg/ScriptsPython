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
def agg_func(group):
    global contador
    contador += 1
    print(f"Procesando grupo {contador}")
    
    dropna_values = group.dropna().unique()
    if len(dropna_values) > 0:
        return dropna_values
    else:
        return None
    
def replace_chars(cell_value):
    # Verificar si el valor de la celda es una cadena de texto
    if isinstance(cell_value, str):
        # Reemplazar ['\\\\ por \\
        cell_value = cell_value.replace("['\\\\\\\\", '\\\\')
        # Reemplazar '] por nada
        cell_value = cell_value.replace("\']", '')
    # else: 
        # print("no se pudo")
    return cell_value
# =============================================================================
# CONSTANTES
# =============================================================================

nombre_archivo = "Consolidado.txt"

# =============================================================================
# Colecciones 
# =============================================================================
lista_df_temp = []

# =============================================================================
# Cargue de BBDD
# =============================================================================
# file_path = filedialog.askopenfilename(defaultextension=".xls")
# if file_path is None:
#     # quit()
#     sys.exit()
# df = pd.read_excel(file_path, header=3)
df1 = pd.read_csv(nombre_archivo, sep='\t')
#df = df.drop(columns = 'Unnamed: 0')
print("Archivo cargado correctamente con "+str(len(df1))+" filas \n")
df = df1.copy()
# =============================================================================
# MAIN
# =============================================================================
warnings.filterwarnings('ignore')
warnings.warn('DelftStack')
warnings.warn('Do not show this message')

del df['Entrega']
del df['FOTO']
"""
df["Territorio"] = df['RUTA'].apply(lambda x: x.split('\\')[7])
df_levMat = df[(df["Territorio"] != "01_ATLÁNTICO_NORTE") & 
               (df["Territorio"] != "02_ATLÁNTICO_SUR") & 
               (df["Territorio"] != "03_MAGDALENA") & 
               (df["Territorio"] != "04_GUAJIRA")]

df_levMat['Territorio2'] = df_levMat['RUTA'].apply(lambda x: x.split('\\')[8])
df_levMatSinduplicados = df_levMat['Territorio2'].drop_duplicates()

df_new = df_levMat[(df_levMat["Territorio2"] == "ATLN") | 
               (df_levMat["Territorio2"] == "MAG")| 
               (df_levMat["Territorio2"] == "GUA") | 
               (df_levMat["Territorio2"] == "ATLS")]
df_newSinduplicados = df_new['Territorio2'].drop_duplicates()

df_territorio = df['RUTA'].apply(lambda x: x.split('\\')[7])
df_territorio.drop_duplicates(inplace=True)
"""
df['RUTA'] = df['RUTA'].replace({
    "01_ATLÁNTICO_NORTE": "ATLN",
    "02_ATLÁNTICO_SUR": "ATLS",
    "03_MAGDALENA": "MAG",
    "04_GUAJIRA": "GUA"
}, regex=True)

"""
df["Territorio"] = df['RUTA'].apply(lambda x: x.split('\\')[7])
df_new1 = df[(df["Territorio"] != "ATLN") & 
               (df["Territorio"] != "MAG") & 
               (df["Territorio"] != "GUA") & 
               (df["Territorio"] != "ATLS")]
df_new1Sinduplicados = df_new1['Territorio'].drop_duplicates()

df_new1['Territorio2'] = df_new1['RUTA'].apply(lambda x: x.split('\\')[8])
df_new2Sinduplicados = df_new1['Territorio2'].drop_duplicates()
"""
df[['codigo', 'Foto']] = df['RENOMBRE FOTO'].str.split('_', n=1, expand=True)
del df['RENOMBRE FOTO']

df_head = df.head(n=100)
pivoted_df = df.pivot_table(index=df.index, columns='Foto', values='RUTA', aggfunc='first')
pivoted_df.columns.name = None

pivoted_df.insert(0,"codigo",0) 
pivoted_df['codigo'] = df['codigo']
df_head1 = pivoted_df.head(n=100)



columnas_ordenadas = sorted(pivoted_df.columns, key=lambda x: int(x.split('_')[1]) if len(x.split('_')) > 1 else -1)
df_ordenado = pivoted_df[columnas_ordenadas]
df_head2 = df_ordenado.head(n=100)

df_asc = df_ordenado.sort_values("codigo", ascending=True)
df_asc = df_asc.reset_index()
df_asc = df_asc.drop(columns = 'index')
df_head3 = df_asc.head(n=1000)

# codigos = df_head3["codigo"].drop_duplicates()
# sel = df_head3[(df_head3["codigo"] == "10000")]
# sel['Foto_2'] = sel['Foto_2'].sort_values()
# nn = 0
#for codigo in codigos:
    # print(str(codigo), str(n))
    #df_head3 = df[(df["Territorio"] != "ATLN")
    

contador = 0
df_agrupado = df_asc.groupby('codigo').agg(agg_func).reset_index()
df_head4 = df_agrupado.head(n=1000)


df_agrupado = df_agrupado.astype(str)
# df_head4.loc[:,"Foto_1"] = df_head4.loc[:,"Foto_1"].astype(str).apply(str.replace,args=("['\\\\\\\\", '\\\\'))
# df_head4.loc[:,"Foto_1"] = df_head4.loc[:,"Foto_1"].astype(str).apply(str.replace,args=("\']", ''))
df_final_2 = df_agrupado.applymap(replace_chars)
df_head6 = df_final_2.head(n=1000)

#df_agrupado.to_csv('fixSF.csv', index=False)
#df_final = pd.read_csv("fixSF.csv")

df_final_2.to_csv('fixSF_final.csv', index=False)

