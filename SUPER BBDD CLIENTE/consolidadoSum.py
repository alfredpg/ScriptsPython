# -*- coding: utf-8 -*-
"""
Created on Wed Jan 25 15:32:03 2023
@author: ALFREDO PADILLA
    -SUPER BBDD CLIENTES-
"""
#Cargamos las librerias
import pandas as pd
from tkinter import filedialog
import time
import sys
import warnings

t0 = time.time()
# =============================================================================
# Funciones
# =============================================================================
def copiarColumnna(columna,posicion,df):
    copia = str(columna) + "-"
    df.insert(posicion,copia,0)
    df[copia] = df[columna]

# =============================================================================
# Cargue de BBDD
# =============================================================================

print("Informe de clientes MLU cargando...... ")
df_clienteMLU = pd.read_csv('C:/Users/P568/Desktop/informes/InformeclientesMLU.csv', encoding="ANSI", delimiter=";", on_bad_lines="skip",low_memory=False)
hClientesMLU = df_clienteMLU.head(n=10)
print("Parte 1/1 cargada...")

print("SUMINISTROS MLU cargando...... ")
df_suministrosMLU_1 = pd.read_excel('SUMINISTROS_10-04-2023.xlsx', sheet_name='1')
print("Parte 1/4 cargada...")
df_suministrosMLU_2 = pd.read_excel('SUMINISTROS_10-04-2023.xlsx', sheet_name='2')
print("Parte 2/4 cargada...")
df_suministrosMLU_3 = pd.read_excel('SUMINISTROS_10-04-2023.xlsx', sheet_name='ENERO_SUM')
print("Parte 3/4 cargada...")
df_suministrosMLU_4 = pd.read_excel('SUMINISTROS_10-04-2023.xlsx', sheet_name='FEBRERO_Y MARZO')
print("Parte 4/4 cargada...")
df_suministrosMLU = pd.concat([df_suministrosMLU_1,df_suministrosMLU_2,df_suministrosMLU_3,df_suministrosMLU_4],axis=0)
df_suministrosMLU.reset_index(inplace=True) #resetar index
df_suministrosMLU = df_suministrosMLU.drop(columns = 'index') #elimina columna index
print("SUMINISTROS MLU Cargados Completamente")
# df_levI = pd.read_excel('C:/Users/P568/Desktop/6. Avance x Cto BDI_31.Ene.23 & GDB_31.Ene.23.xlsx', sheet_name='Lev I',header=8)
# df_levI = df_levI.drop(columns = 'Unnamed: 0')

# =============================================================================
# VALIDAR REGISTROS DUPLICADOS Y ERRONEOS EN BBDD
# =============================================================================
print(" ")
print("Validando informe clientes...")
df_clienteMLU["val_duplicados"] = df_clienteMLU.duplicated(subset=["EquipoRutaId"], keep=False)
df_clienteMLU_dup = df_clienteMLU[df_clienteMLU['val_duplicados'] == True ]
df_clienteMLU_ok = df_clienteMLU[df_clienteMLU['val_duplicados'] == False ]
print("Se encontraron "+str(len(df_clienteMLU_dup)) +" ID duplicados en informe clientes")
print("Se encontraron "+str(len(df_clienteMLU_ok)) +" ID ok en informe clientes")
#Eliminar duplicados equipo ruta id
df_clienteMLU = df_clienteMLU.drop_duplicates(subset=['EquipoRutaId']) 

print(" ")
print("Validando SUMINISTROS MLU...")
df_suministrosMLU["val_duplicados"] = df_suministrosMLU.duplicated(subset=["ID"], keep=False)
df_suministrosMLU_dup = df_suministrosMLU[df_suministrosMLU['val_duplicados'] == True ]
df_suministrosMLU_ok = df_suministrosMLU[df_suministrosMLU['val_duplicados'] == False ]
print("Se encontraron "+str(len(df_suministrosMLU_dup)) +" ID duplicados en SUMINISTROS MLU")
print("Se encontraron "+str(len(df_suministrosMLU_ok)) +" ID ok en SUMINISTROS MLU")

# =============================================================================
# sumMLU = suministrosMLU + clienteMLU
# =============================================================================
df_suministrosMLU.insert(2, "EquipoRutaId", 0)
df_suministrosMLU["CODIGO"] = df_suministrosMLU["CODIGO"].astype(str)
df_suministrosMLU["EquipoRutaId"] = df_suministrosMLU["CODIGO"].str[-8:]  

df_suministrosMLU["EquipoRutaId"] = df_suministrosMLU["EquipoRutaId"].astype(str)

print(" ")
print("GENERANDO... sumMLU = suministrosMLU + clienteMLU")
df_clienteMLU["EquipoRutaId"] = df_clienteMLU["EquipoRutaId"].astype(str)
df_suministrosMLU["EquipoRutaId"] = df_suministrosMLU["EquipoRutaId"].astype(str)
sumMLU = df_suministrosMLU.merge(df_clienteMLU[["EquipoRutaId","Estado del número del medidor",'Estado del sticker','rutaid']], how='left', on='EquipoRutaId')

# =============================================================================
# fin -sumMLU
# =============================================================================
sumMLU['rutaid'].fillna(0, inplace=True)
sumMLU['TERRITORIO'].fillna(0, inplace=True)
sumMLURutaid0 = sumMLU[sumMLU['rutaid'] == 0]
sumMLU.loc[sumMLURutaid0.index,"rutaid"] =  sumMLU.loc[:,"TERRITORIO"]

# sumMLU['rutaid'].fillna(0, inplace=True)
# sumMLURutaid0 = sumMLU[sumMLU['rutaid'] == 0]

sumMLU['TERRITORIO'] = sumMLU['rutaid']
del sumMLU['rutaid'] #ELIMINAR COLUMNA CODELEME 

# sumMLU['TERRITORIO'].fillna(0, inplace=True)
# sumMLURutaid0 = sumMLU[sumMLU['TERRITORIO'] == 0]

sumMLU['TERRITORIO'].fillna(0, inplace=True)
departamentos = sumMLU['TERRITORIO']
departamentos.drop_duplicates(inplace=True)

#reperando atln
atln = sumMLU[sumMLU["TERRITORIO"].str[:2] == "01"]
sumMLU.loc[atln.index,"TERRITORIO"] =  "01. ATLANTICO NORTE"

total = 0
for i in departamentos:
    #print(i)
    if (i != 0) :
        nombreArchivo = "sumMlu"+str(i[4:]+".csv")
        nombreArchivo = nombreArchivo.replace(' ', '_') #remplazamos
        clientesMlu_i = sumMLU[sumMLU["TERRITORIO"] == i]    
        print(nombreArchivo, len(clientesMlu_i))
        cantSum = len(clientesMlu_i)
        clientesMlu_i.to_csv(path_or_buf=nombreArchivo, sep=';', index=False)
        total = total + cantSum
        print(total,"\n")
       
    else: 
        next

