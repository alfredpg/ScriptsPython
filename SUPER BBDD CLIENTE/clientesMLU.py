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
# Cargue de BBDD
# =============================================================================
print("Informe de clientes MLU cargando...... ")
df_clienteMLU = pd.read_csv('C:/Users/P568/Desktop/informes/InformeclientesMLU.csv', encoding="ANSI", delimiter=";", on_bad_lines="skip",low_memory=False)
x = df_clienteMLU.head(n=10)
print("Parte 1/1 cargada...")

print("SUMINISTROS MLU cargando...... ")
df_suministrosMLU_1 = pd.read_excel('C:/Users/P568/Desktop/informes/SUMINISTROS_20230131_02 - copia.xlsx', sheet_name='1')
print("Parte 1/3 cargada...")
df_suministrosMLU_2 = pd.read_excel('C:/Users/P568/Desktop/informes/SUMINISTROS_20230131_02 - copia.xlsx', sheet_name='2')
print("Parte 2/3 cargada...")
df_suministrosMLU_3 = pd.read_excel('C:/Users/P568/Desktop/informes/SUMINISTROS_20230131_02 - copia.xlsx', sheet_name='enero')
print("Parte 3/3 cargada...")
df_suministrosMLU = pd.concat([df_suministrosMLU_1,df_suministrosMLU_2,df_suministrosMLU_3],axis=0)
df_suministrosMLU.reset_index(inplace=True) #resetar index
df_suministrosMLU = df_suministrosMLU.drop(columns = 'index') #elimina columna index
print("SUMINISTROS MLU Cargados Completamente")

df_levI = pd.read_excel('C:/Users/P568/Desktop/6. Avance x Cto BDI_31.Ene.23 & GDB_31.Ene.23.xlsx', sheet_name='Lev I',header=8)
df_levI = df_levI.drop(columns = 'Unnamed: 0')




# =============================================================================
# VALIDAR REGISTROS DUPLICADOS Y ERRONEOS EN BBDD
# =============================================================================
print(" ")
print("Validando informe clientes...")
df_clienteMLU["val_duplicados"] = df_clienteMLU.duplicated(subset=["EquipoRutaId"], keep=False)
df_clienteMLU_dup = df_clienteMLU[df_clienteMLU['val_duplicados'] == True ]
df_clienteMLU_ok = df_clienteMLU[df_clienteMLU['val_duplicados'] == False ]
print("Se encontraron "+str(len(df_clienteMLU_dup)) +" ID duplicados en informe clientes")
print("Se encontraron "+str(len(df_clienteMLU_ok)) +" ID duplicados en informe clientes")
#Eliminar duplicados equipo ruta id
df_clienteMLU = df_clienteMLU.drop_duplicates(subset=['EquipoRutaId']) 

print(" ")
print("Validando SUMINISTROS MLU...")
df_suministrosMLU["val_duplicados"] = df_suministrosMLU.duplicated(subset=["EquipoRutaId"], keep=False)
df_suministrosMLU_dup = df_suministrosMLU[df_suministrosMLU['val_duplicados'] == True ]
df_suministrosMLU_ok = df_suministrosMLU[df_suministrosMLU['val_duplicados'] == False ]
print("Se encontraron "+str(len(df_suministrosMLU_dup)) +" ID duplicados en SUMINISTROS MLU")
print("Se encontraron "+str(len(df_suministrosMLU_ok)) +" ID duplicados en SUMINISTROS MLU")

# =============================================================================
# sumMLU = suministrosMLU + clienteMLU
# =============================================================================
print(" ")
print("GENERANDO... sumMLU = suministrosMLU + clienteMLU")
df_clienteMLU["EquipoRutaId"] = df_clienteMLU["EquipoRutaId"].astype(str)
df_suministrosMLU["EquipoRutaId"] = df_suministrosMLU["EquipoRutaId"].astype(str)
sumMLU = df_suministrosMLU.merge(df_clienteMLU[["EquipoRutaId","NombreTrabajo",'ID_BDI']], how='left', on='EquipoRutaId')

freq = sumMLU['INSTALACIO'].value_counts()
freq = freq.reset_index()
freq.rename(columns={'index': 'INSTALACIO',
                     'INSTALACIO': 'Recuento'}, inplace=True) #RENOMBRANDO COLUMNA

print(" ")
print("GENERANDO... sumMLU1 = sumMLU + freq")
sumMLU["INSTALACIO"] = sumMLU["INSTALACIO"].astype(str)
freq["INSTALACIO"] = freq["INSTALACIO"].astype(str)
sumMLU1 = sumMLU.merge(freq, how='left', on='INSTALACIO' )

sumMLU1.rename(columns={'NombreTrabajo': 'Ruta',
                        'ID_BDI': 'PLACA'}, inplace=True) #RENOMBRANDO COLUMNA

# =============================================================================
# 
# =============================================================================
df_levI.insert(2, "cod-cant", 0)
df_levI["cod-cant"] = df_levI["INSTALACION_ORIGEN"].astype(str) + df_levI["Total Clientes"].astype(str)

sumMLU1.insert(4, "cod-cant", 0)
sumMLU1["cod-cant"] = sumMLU1["INSTALACIO"].astype(str) + sumMLU1["Recuento"].astype(str)
sumMLUplacas = sumMLU1.drop_duplicates(subset=['cod-cant','Ruta','PLACA'])


df_levI_ok = df_levI.merge(sumMLUplacas[["cod-cant","Ruta",'PLACA']], how='left', on='cod-cant')
df_levI_ok.to_excel('C:/Users/P568/Desktop/levIPlacas.xlsx', sheet_name='Lev I', index = False)

# df_clienteMLU["placa vs ruta"] = df_clienteMLU["NombreTrabajo"] == df_clienteMLU["ID_BDI"]
aa = df_clienteMLU[df_clienteMLU["NombreTrabajo"] == "A30501_SP"]
bb = sumMLU1[sumMLU1["INSTALACIO"] == '65742056']


sumMLUplacas.to_excel('C:/Users/P568/Desktop/sumMLUplacas.xlsx', sheet_name='sumMLUplacas', index = False)


# =============================================================================
# 
# =============================================================================

df = sumMLU1.reset_index()
df1 = df[df['index'] <= 999999] #filtramos ID_BDI en cero
df2 = df[df['index'] > 999999] #filtramos ID_BDI en cero

df1 = df1.drop(columns = 'index') #elimina columna index
df2 = df2.drop(columns = 'index') #elimina columna index


#imprimimos el df en un excel
df1.to_excel('sumMLU_1.xlsx', sheet_name='parte 1', index = False)
df2.to_excel('sumMLU_2.xlsx', sheet_name='parte 2', index = False)


