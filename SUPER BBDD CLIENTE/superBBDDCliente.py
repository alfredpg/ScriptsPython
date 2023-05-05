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


# =============================================================================
# mejoras
# =============================================================================
# - duplicados de qr
# - eliminar regstris de serie ant en med

# =============================================================================
# Funciones
# =============================================================================
def copiarColumnna(columna,posicion,df):
    copia = str(columna) + "-"
    df.insert(posicion,copia,0)
    df[copia] = df[columna]



t0 = time.time()
# =============================================================================
# Cargue de BBDD
# =============================================================================
print("BBDD 530 cargando...... ")
df_530_1 = pd.read_excel('BBDD Cierre ENERO23.xlsx', sheet_name='530-1')
print("Parte 1 cargada...")
df_530_2 = pd.read_excel('BBDD Cierre ENERO23.xlsx', sheet_name='530-2')
print("Parte 2 cargada...")
df_530 = pd.concat([df_530_1,df_530_2],axis=0)
df_530.reset_index(inplace=True) #resetar index
df_530 = df_530.drop(columns = 'index') #elimina columna index
print("BBDD 530 Cargada Completamente")
print(" ")
print("BBDD medidores cargando...... ")
df_med_1 = pd.read_excel('BBDD Cierre ENERO23.xlsx', sheet_name='MEDIDOR 1')
print("Parte 1 cargada...")
df_med_2 = pd.read_excel('BBDD Cierre ENERO23.xlsx', sheet_name='MEDIDOR 2')
print("Parte 2 cargada...")
df_med = pd.concat([df_med_1,df_med_2],axis=0)
df_med.reset_index(inplace=True) #resetar index
df_med = df_med.drop(columns = 'index') #elimina columna index
print("BBDD mededidores Cargada")
print(" ")
print("BBDD direcciones cargando...... ")
df_dir_1 = pd.read_excel('BBDD Cierre ENERO23.xlsx', sheet_name='DIR 1')
print("Parte 1 cargada...")
df_dir_2 = pd.read_excel('BBDD Cierre ENERO23.xlsx', sheet_name='DIR 2')
print("Parte 2 cargada...")
df_dir = pd.concat([df_dir_1,df_dir_2],axis=0)
df_dir.reset_index(inplace=True) #resetar index
df_dir = df_dir.drop(columns = 'index') #elimina columna index
print("BBDD direcciones Cargada")
print(" ")
print("BBDD QR cargando...... ")
df_qr = pd.read_excel('20211118 Compilado de códigos QR.xlsx')
print("BBDD QR Cargada")
print(" ")
print("BBDD osf cargando...... ")
df_osf_1 = pd.read_csv('Segmentación_AMT_202212_atl.csv', encoding="ANSI", delimiter=";", on_bad_lines="skip",low_memory=False)
print("Parte 1 cargada...")
df_osf_2 = pd.read_csv('Segmentación_AMT_202212_norte.csv', encoding="ANSI", delimiter=";", on_bad_lines="skip",low_memory=False)
print("Parte 2 cargada...")
df_osf = pd.concat([df_osf_1,df_osf_2],axis=0)
df_osf.reset_index(inplace=True) #resetar index
df_osf = df_osf.drop(columns = 'index') #elimina columna index
print("BBDD osf Cargada")

# tiempo de cargue
a = time.time() - t0
tiempoCargue = '{0:.2f}'.format(a)
print("El tiempo de cargue es: " + str(tiempoCargue) + " seg")

# fecha de las BBDD
fecha = "23 ENE"

# =============================================================================
# VALIDAR REGISTROS DUPLICADOS Y ERRONEOS EN BBDD
# =============================================================================

#validar duplicados y errores en 530
print(" ")
print("Validando 530...")
df_530["NRO_ACOMETIDA"].fillna("NN", inplace=True) #cambiar vacias por 0 en ACOMETIDA
df_530Error = df_530[df_530["NRO_ACOMETIDA"] == "NN" ] #seleccionar ACOMETIDAS en 0
df_530.drop(df_530Error.index , inplace=True) #eliminar seleccion pasada por que son registros erroneos
print(str(len(df_530Error)) +" registros erroneos eliminados 530")
df_530["TRANSFORMADOR"].fillna(0, inplace=True) 
df_530SinTx = df_530[df_530["TRANSFORMADOR"] == 0 ] #seleccionar ACOMETIDAS en 0
df_530.drop(df_530SinTx.index , inplace=True)
print(str(len(df_530SinTx)) +" registros SIN TRANSFORMADOR eliminados 530")

#     #duplicados
# df_530["val_duplicados"] = df_530.duplicated(subset=["NIC"], keep=False)
# df_530_dup = df_530[df_530['val_duplicados'] == True ]
# df_530_ok = df_530[df_530['val_duplicados'] == False ]
# print("Se encontraron "+str(len(df_530_dup)) +" NIC´S duplicados en 530")
# print("Se encontraron "+str(len(df_530_ok)) +" NIC´S OK en 530")

    #duplicados
df_530["val_duplicados_NIS"] = df_530.duplicated(subset=["NIS_RAD"], keep=False)
df_530_dup_NIS = df_530[df_530['val_duplicados_NIS'] == True ]
df_530_ok_NIS = df_530[df_530['val_duplicados_NIS'] == False ]
print("Se encontraron "+str(len(df_530_dup_NIS)) +" NIS´S duplicados en 530")
print("Se encontraron "+str(len(df_530_ok_NIS)) +" NIS´S OK en 530")
df_530_dup_NIS.to_csv(path_or_buf='duplicados_530.csv', sep=';', index=False)

#validar duplicados y errores en osf
# print(" ")
# print("validado OSF...")
# df_osf["val_duplicados"] = df_osf.duplicated(subset=["NIC"], keep=False)
# df_osf_dup = df_osf[df_osf['val_duplicados'] == True ]
# df_osf_ok = df_osf[df_osf['val_duplicados'] == False ]
# print("Se encontraron "+str(len(df_osf_dup)) +" NIC´S duplicados en OSF")
# print("Se encontraron "+str(len(df_osf_ok)) +" NIC´S OK en OSF")

print(" ")
print("validado OSF...")
df_osf["val_duplicados"] = df_osf.duplicated(subset=["NIS_RAD"], keep=False)
df_osf_dup_NIS = df_osf[df_osf['val_duplicados'] == True ]
df_osf_ok_NIS = df_osf[df_osf['val_duplicados'] == False ]
print("Se encontraron "+str(len(df_osf_dup_NIS)) +" NIS´S duplicados en OSF")
print("Se encontraron "+str(len(df_osf_ok_NIS)) +" NIS´S OK en OSF")

#validar duplicados y errores en medidores
print(" ")
print("validado MEDIDORES...")
df_med["Unnamed: 7"].fillna(0, inplace=True) #cambiar vacias por 0
sel =df_med[df_med["Unnamed: 7"] != 0] #filtrar 0
#concatenar el desborde con la culumna anterior
df_med.loc[sel.index, "DESCRIPCION_FINCA"] = df_med["DESCRIPCION_FINCA"].astype(str)+" "+df_med["Unnamed: 7"].astype(str) 
del df_med["Unnamed: 7"] #eliminar columna
df_med["val_duplicados"] = df_med.duplicated(subset=["NIS_RAD"], keep=False)
df_med_dup = df_med[df_med['val_duplicados'] == True ]
df_med_ok = df_med[df_med['val_duplicados'] == False ]
print("Se encontraron "+str(len(df_med_dup)) +" NIS_RAD´S duplicados en MEDIDORES")
print("Se encontraron "+str(len(df_med_ok)) +" NIS_RAD´S OK en MEDIDORES")

#validar duplicados y errores en dir
# print(" ")
# print("validado DIRECCIONES...")
# df_dir["val_duplicados"] = df_dir.duplicated(subset=["NIC"], keep=False)
# df_dir_dup = df_dir[df_dir['val_duplicados'] == True ]
# df_dir_ok = df_dir[df_dir['val_duplicados'] == False ]
# print("Se encontraron "+str(len(df_dir_dup)) +" NIC´S duplicados en DIRECCIONES")
# print("Se encontraron "+str(len(df_dir_ok)) +" NIC´S OK en DIRECCIONES")

print(" ")
print("validado DIRECCIONES...")
df_dir["val_duplicados"] = df_dir.duplicated(subset=["NIS_RAD"], keep=False)
df_dir_dup_NIS = df_dir[df_dir['val_duplicados'] == True ]
df_dir_ok_NIS = df_dir[df_dir['val_duplicados'] == False ]
print("Se encontraron "+str(len(df_dir_dup_NIS)) +" NIS´S duplicados en DIRECCIONES")
print("Se encontraron "+str(len(df_dir_ok_NIS)) +" NIS´S OK en DIRECCIONES")


#validar duplicados y errores en qr
print(" ")
print("validado QR...")
df_qr["NIC"].fillna(0, inplace=True)
df_qr["NIS"].fillna(0, inplace=True)
df_qr["NIC"] = df_qr["NIC"].astype("int64") 
df_qr["NIS"] = df_qr["NIS"].astype("int64") 
sel = df_qr[df_qr["NIS"] == 0]
sel = sel[sel["NIC"] == 0]
df_qr.drop(sel.index , inplace=True) #eliminar seleccion pasada por que son registros erroneos
print(str(len(sel)) +" registros erroneos eliminados QR, son NIC 0 y NIS 0")

sel = df_qr[df_qr["NIC"] == 0]
df_qrNis = sel[sel["NIS"] != 0]

df_qrNic = df_qr[df_qr["NIC"] != 0]

df_qrNic["val_duplicados"] = df_qrNic.duplicated(subset=["NIC"], keep=False)
df_qrNic_dup = df_qrNic[df_qrNic['val_duplicados'] == True ]
df_qrNic_ok = df_qrNic[df_qrNic['val_duplicados'] == False ]
print("Se encontraron "+str(len(df_qrNic_dup)) +" NIC´S duplicados en QR")
print("Se encontraron "+str(len(df_qrNic_ok)) +" NIC´S OK en QR")

print("Cuando NIC = 0 se encontraron: ")
df_qrNis["val_duplicados"] = df_qrNis.duplicated(subset=["NIS"], keep=False)
df_qrNis_dup = df_qrNis[df_qrNis['val_duplicados'] == True ]
df_qrNis_ok = df_qrNis[df_qrNis['val_duplicados'] == False ]
print(str(len(df_qrNis_dup)) +" NIC´S duplicados en QR")
print(str(len(df_qrNis_ok)) +" NIC´S OK en QR")

# =============================================================================
# Ensamble SUPERBBDD = 530 + OSF
# =============================================================================
print(" ")
print("GENERANDO... SUPERBBDD = 530 + OSF")
df_530_ok_NIS["NIC"].fillna(0, inplace=True)
df_osf_ok_NIS["NIC"].fillna(0, inplace=True)
df_530_ok_NIS["NIC"] = df_530_ok_NIS["NIC"].astype("int64") 
df_osf_ok_NIS["NIC"] = df_osf_ok_NIS["NIC"].astype("int64")
df_530_ok_NIS["NIS_RAD"].fillna(0, inplace=True)
df_osf_ok_NIS["NIS_RAD"].fillna(0, inplace=True)
df_530_ok_NIS["NIS_RAD"] = df_530_ok_NIS["NIS_RAD"].astype("int64") 
df_osf_ok_NIS["NIS_RAD"] = df_osf_ok_NIS["NIS_RAD"].astype("int64")
#unir 530 y OSF por NIC conservando la diferencia de los dos
df_superBBDD = df_530_ok_NIS.merge(df_osf_ok_NIS, how='outer', on='NIS_RAD')
df_superBBDD["DESC_ASIGNACION"].fillna("A", inplace=True) #cambiar vacias por A



#columnas de verificacion y seguimiento
df_superBBDD.insert(0,"Periodo","-") #Insertar columna periodo
df_superBBDD.insert(1,"BBDD","-") #insertar columna BBDD
df_superBBDD.insert(2,"Criterios","-") #insertar columna Criterios

#crear df de los que no traen NIS_RAD por que no cruzaron con OSF
BDI = df_superBBDD[df_superBBDD['DESC_ASIGNACION'] == "A"]
df_superBBDD.loc[BDI.index,"BBDD"] = "BDI"  #colocar BDI en columna BBDD
df_superBBDD.loc[BDI.index,"Criterios"] = "N/A" #No aplican para criterio

#crear df de los que ACOMETIDA es cero por que esos son los que no estaban en BDI son meramente OSF
df_superBBDD["NRO_ACOMETIDA"].fillna(0, inplace=True) #cambiar vacias por 0 en ACOMETIDA
OSF = df_superBBDD[df_superBBDD['NRO_ACOMETIDA'] == 0 ] #filtrar 0
df_superBBDD.loc[OSF.index,"BBDD"] = "OSF" #colocar OSF en BBDD
    #traer los datos del cruce de OSF
df_superBBDD.loc[OSF.index,"Criterios"] = "N/A"
df_superBBDD.loc[OSF.index,"TRANSFORMADOR"] = df_superBBDD.loc[:,"CODIGO_TRAFO_MB"]
df_superBBDD.loc[OSF.index,"DEPARTAMENTO"] = df_superBBDD.loc[:,"TERRITORIO"]
df_superBBDD.loc[OSF.index,"NIC_x"] = df_superBBDD.loc[:,"NIC_y"]
df_superBBDD.loc[OSF.index,"DEPARTAMENTO_CLI"] = df_superBBDD.loc[:,"NOM_DEPTO"]
df_superBBDD.loc[OSF.index,"MUNICIPIO_CLI"] = df_superBBDD.loc[:,"NOM_MUNIC"]
df_superBBDD.loc[OSF.index,"CORREGIMIENTO_CLI"] = df_superBBDD.loc[:,"NOM_CORREG"]
df_superBBDD.loc[OSF.index,"BARRIO"] = df_superBBDD.loc[:,"NOM_LOCAL"]
df_superBBDD.loc[OSF.index,"NOMBRE_CLIENTE"] = df_superBBDD.loc[:,"NOMBRE"]
df_superBBDD.loc[OSF.index,"LOCALIZACION"] = df_superBBDD.loc[:,"DIRECCION"]

#crear df con los que NO cumplen las condiciones pasadas
BDIOSF = df_superBBDD[df_superBBDD['BBDD'] == "-" ] #filtrar 0
df_superBBDD.loc[BDIOSF.index,"BBDD"] = "BDI/OSF" #colocar BDI/OSF en BBDD
    #logica para los criterios
        # Val: NIS_RAD
df_superBBDD.insert(3,"Val_NIC","N/A")
df_superBBDD.loc[BDIOSF.index,"Val_NIC"] = BDIOSF["NIC_x"] == BDIOSF["NIC_y"]
df_superBBDD["Val_NIC"].replace(False, 'NO', inplace=True)
df_superBBDD["Val_NIC"].replace(True, 'SI', inplace=True)
df_superBBDD["Val_NIC"].fillna('N/A', inplace=True)

        # Val: MUNICIPIO
df_superBBDD.insert(4,"Val_MUNIC","N/A")
df_superBBDD.loc[BDIOSF.index,"Val_MUNIC"] = BDIOSF["MUNICIPIO_CLI"] == BDIOSF["NOM_MUNIC"]
df_superBBDD["Val_MUNIC"].replace(False, 'NO', inplace=True)
df_superBBDD["Val_MUNIC"].replace(True, 'SI', inplace=True)
df_superBBDD["Val_MUNIC"].fillna('N/A', inplace=True)

#         # Val: CLIENTE
# df_superBBDD.insert(5,"Val_CLIENTE","N/A")
# df_superBBDD.loc[BDIOSF.index,"Val_CLIENTE"] = BDIOSF["NOMBRE_CLIENTE"] == BDIOSF["NOMBRE"]
# df_superBBDD["Val_CLIENTE"].replace(False, 'NO', inplace=True)
# df_superBBDD["Val_CLIENTE"].replace(True, 'SI', inplace=True)
# df_superBBDD["Val_CLIENTE"].fillna('N/A', inplace=True)

#         # Val: DIRECCION
# df_superBBDD.insert(6,"Val_DIRECC","N/A")
# df_superBBDD.loc[BDIOSF.index,"Val_DIRECC"] = BDIOSF["DIRECCION"] == BDIOSF["DIRECCION_SUMINISTRO"]
# df_superBBDD["Val_DIRECC"].replace(False, 'NO', inplace=True)
# df_superBBDD["Val_DIRECC"].replace(True, 'SI', inplace=True)
# df_superBBDD["Val_DIRECC"].fillna('N/A', inplace=True)
        
        #concatennar los criterios
# df_superBBDD.loc[BDIOSF.index,"Criterios"] = "Nic: Si, Nis_R: "+df_superBBDD["Val_NIS_RAD"]+", Mun: "+df_superBBDD["Val_MUNIC"]+", Clt: "+df_superBBDD["Val_CLIENTE"]+", Dir: "+df_superBBDD["Val_DIRECC"]
df_superBBDD.loc[BDIOSF.index,"Criterios"] = "Nis: Si, Nic: "+df_superBBDD["Val_NIC"]+", Mun: "+df_superBBDD["Val_MUNIC"]

#eliminar las siguientes columnas
df_superBBDD = df_superBBDD.drop(columns=['val_duplicados_NIS','NIC_y',"NOMBRE",'TARIFA', 'COD_LOCAL','NOM_LOCAL',"NOM_MUNIC","NOM_CORREG",'NOM_DEPTO',
                                          'DESC_ASIGNACION','DESC_PROD_STATUS','LONGITUD','LATITUD','CSMO_ACTIVA','CODIGO_CIRCUITO','CODIGO_TRAFO_MB',
                                          'TERRITORIO','val_duplicados'])

df_superBBDD["Criterios"] = df_superBBDD["Criterios"].astype(str)
#renombrar columnas
df_superBBDD.rename(columns={'DIRECCION':'DIRECCION_OSF','NIC_x':'NIC' }, inplace=True)

# =============================================================================
# SUPERBBDD2 = SUPERBBDD + MEDIDORES
# =============================================================================
print(" ")
print("GENERANDO... SUPERBBDD2 = SUPERBBDD + MEDIDORES")
#Cruzar solo los nis presentes en SUPERBBDD
df_superBBDD2 = df_superBBDD.merge(df_med, how='left', on='NIS_RAD')
df_superBBDD2 = df_superBBDD2.drop(columns=['NRO_ACOMETIDA_y','NIF_y'])
df_superBBDD2.rename(columns={'val_duplicados':'¿NIS DUPLICADO EN MED?'}, inplace=True)
df_superBBDD2['¿NIS DUPLICADO EN MED?'].replace(False, 'NO', inplace=True)
df_superBBDD2['¿NIS DUPLICADO EN MED?'].replace(True, 'SI', inplace=True)
df_superBBDD2['¿NIS DUPLICADO EN MED?'].fillna('N/A', inplace=True)

df_superBBDD2_BDI = df_superBBDD2[df_superBBDD2['BBDD'] == 'BDI' ]
df_superBBDD2_BDI_NIS_NA = df_superBBDD2_BDI[df_superBBDD2_BDI['¿NIS DUPLICADO EN MED?'] == 'N/A' ]
df_superBBDD2_BDI_NIS_OK = df_superBBDD2_BDI[df_superBBDD2_BDI['¿NIS DUPLICADO EN MED?'] == 'NO' ]
df_superBBDD2_BDI_NIS_DUP = df_superBBDD2_BDI[df_superBBDD2_BDI['¿NIS DUPLICADO EN MED?'] == 'SI' ]

df_superBBDD2_OSF = df_superBBDD2[df_superBBDD2['BBDD'] == 'OSF' ]
df_superBBDD2_OSF_NIS_NA = df_superBBDD2_OSF[df_superBBDD2_OSF['¿NIS DUPLICADO EN MED?'] == 'N/A' ]
df_superBBDD2_OSF_NIS_OK = df_superBBDD2_OSF[df_superBBDD2_OSF['¿NIS DUPLICADO EN MED?'] == 'NO' ]
df_superBBDD2_OSF_NIS_DUP = df_superBBDD2_OSF[df_superBBDD2_OSF['¿NIS DUPLICADO EN MED?'] == 'SI' ]

df_superBBDD2_BDIOSF = df_superBBDD2[df_superBBDD2['BBDD'] == 'BDI/OSF' ]
df_superBBDD2_BDIOSF_NIS_NA = df_superBBDD2_BDIOSF[df_superBBDD2_BDIOSF['¿NIS DUPLICADO EN MED?'] == 'N/A' ]
df_superBBDD2_BDIOSF_NIS_OK = df_superBBDD2_BDIOSF[df_superBBDD2_BDIOSF['¿NIS DUPLICADO EN MED?'] == 'NO' ]
df_superBBDD2_BDIOSF_NIS_DUP = df_superBBDD2_BDIOSF[df_superBBDD2_BDIOSF['¿NIS DUPLICADO EN MED?'] == 'SI' ]

# =============================================================================
# SUPERBBDD3 = SUPERBBDD2 + QRNIS
# =============================================================================
print(" ")
print("GENERANDO... SUPERBBDD3 = SUPERBBDD2 + QRNIS")
df_superBBDD3 = df_superBBDD2.merge(df_qrNis[["NIS","QR DEPURADO",'val_duplicados']], how='left', left_on="NIS_RAD", right_on='NIS')
df_superBBDD3.rename(columns={'val_duplicados':'¿NIS DUPLICADO EN QR?'}, inplace=True)
df_superBBDD3 = df_superBBDD3.drop(columns=["NIS"])
df_superBBDD3['¿NIS DUPLICADO EN QR?'].replace(False, 'NO', inplace=True)
df_superBBDD3['¿NIS DUPLICADO EN QR?'].replace(True, 'SI', inplace=True)
df_superBBDD3['¿NIS DUPLICADO EN QR?'].fillna('N/A', inplace=True)

df_superBBDD3_BDI = df_superBBDD3[df_superBBDD3['BBDD'] == 'BDI' ]
df_superBBDD3_BDI_NIS_NA = df_superBBDD3_BDI[df_superBBDD3_BDI['¿NIS DUPLICADO EN QR?'] == 'N/A' ]
df_superBBDD3_BDI_NIS_OK = df_superBBDD3_BDI[df_superBBDD3_BDI['¿NIS DUPLICADO EN QR?'] == 'NO' ]
df_superBBDD3_BDI_NIS_DUP = df_superBBDD3_BDI[df_superBBDD3_BDI['¿NIS DUPLICADO EN QR?'] == 'SI' ]

df_superBBDD3_OSF = df_superBBDD3[df_superBBDD3['BBDD'] == 'OSF' ]
df_superBBDD3_OSF_NIS_NA = df_superBBDD3_OSF[df_superBBDD3_OSF['¿NIS DUPLICADO EN QR?'] == 'N/A' ]
df_superBBDD3_OSF_NIS_OK = df_superBBDD3_OSF[df_superBBDD3_OSF['¿NIS DUPLICADO EN QR?'] == 'NO' ]
df_superBBDD3_OSF_NIS_DUP = df_superBBDD3_OSF[df_superBBDD3_OSF['¿NIS DUPLICADO EN QR?'] == 'SI' ]

df_superBBDD3_BDIOSF = df_superBBDD3[df_superBBDD3['BBDD'] == 'BDI/OSF' ]
df_superBBDD3_BDIOSF_NIS_NA = df_superBBDD3_BDIOSF[df_superBBDD3_BDIOSF['¿NIS DUPLICADO EN QR?'] == 'N/A' ]
df_superBBDD3_BDIOSF_NIS_OK = df_superBBDD3_BDIOSF[df_superBBDD3_BDIOSF['¿NIS DUPLICADO EN QR?'] == 'NO' ]
df_superBBDD3_BDIOSF_NIS_DUP = df_superBBDD3_BDIOSF[df_superBBDD3_BDIOSF['¿NIS DUPLICADO EN QR?'] == 'SI' ]

# =============================================================================
# SUPERBBDD4 = SUPERBBDD3 + QRNIC
# =============================================================================
print(" ")
print("GENERANDO... SUPERBBDD4 = SUPERBBDD3 + QRNIC")
df_superBBDD4 = df_superBBDD3.merge(df_qrNic[["NIC","QR DEPURADO",'val_duplicados']], how='left', on='NIC', suffixes=('', '_y'))
df_superBBDD4.rename(columns={'val_duplicados':'¿NIC DUPLICADO EN QR?'}, inplace=True)
df_superBBDD4['¿NIC DUPLICADO EN QR?'].replace(False, 'NO', inplace=True)
df_superBBDD4['¿NIC DUPLICADO EN QR?'].replace(True, 'SI', inplace=True)
df_superBBDD4['¿NIC DUPLICADO EN QR?'].fillna('N/A', inplace=True)

df_superBBDD4["QR DEPURADO"].fillna(0, inplace=True)
sel = df_superBBDD4[df_superBBDD4["QR DEPURADO"] == 0]
df_superBBDD4.loc[sel.index,"QR DEPURADO"] = df_superBBDD4.loc[:,"QR DEPURADO_y"]
df_superBBDD4 = df_superBBDD4.drop(columns=["QR DEPURADO_y"])
df_superBBDD4["QR DEPURADO"].fillna(0, inplace=True)

df_superBBDD4_BDI = df_superBBDD4[df_superBBDD4['BBDD'] == 'BDI' ]
df_superBBDD4_BDI_NIC_NA = df_superBBDD4_BDI[df_superBBDD4_BDI['¿NIC DUPLICADO EN QR?'] == 'N/A' ]
df_superBBDD4_BDI_NIC_OK = df_superBBDD4_BDI[df_superBBDD4_BDI['¿NIC DUPLICADO EN QR?'] == 'NO' ]
df_superBBDD4_BDI_NIC_DUP = df_superBBDD4_BDI[df_superBBDD4_BDI['¿NIC DUPLICADO EN QR?'] == 'SI' ]

df_superBBDD4_OSF = df_superBBDD4[df_superBBDD4['BBDD'] == 'OSF' ]
df_superBBDD4_OSF_NIC_NA = df_superBBDD4_OSF[df_superBBDD4_OSF['¿NIC DUPLICADO EN QR?'] == 'N/A' ]
df_superBBDD4_OSF_NIC_OK = df_superBBDD4_OSF[df_superBBDD4_OSF['¿NIC DUPLICADO EN QR?'] == 'NO' ]
df_superBBDD4_OSF_NIC_DUP = df_superBBDD4_OSF[df_superBBDD4_OSF['¿NIC DUPLICADO EN QR?'] == 'SI' ]

df_superBBDD4_BDIOSF = df_superBBDD4[df_superBBDD4['BBDD'] == 'BDI/OSF' ]
df_superBBDD4_BDIOSF_NIC_NA = df_superBBDD4_BDIOSF[df_superBBDD4_BDIOSF['¿NIC DUPLICADO EN QR?'] == 'N/A' ]
df_superBBDD4_BDIOSF_NIC_OK = df_superBBDD4_BDIOSF[df_superBBDD4_BDIOSF['¿NIC DUPLICADO EN QR?'] == 'NO' ]
df_superBBDD4_BDIOSF_NIC_DUP = df_superBBDD4_BDIOSF[df_superBBDD4_BDIOSF['¿NIC DUPLICADO EN QR?'] == 'SI' ]

# =============================================================================
# SUPERBBDD5 = SUPERBBDD4 + DIRECCIONES
# =============================================================================
print(" ")
print("GENERANDO... SUPERBBDD5 = SUPERBBDD4 + DIRECCIONES")
df_superBBDD5 = df_superBBDD4.merge(df_dir[["NIS_RAD",'TIPO VIA O CALLE','CALLE O VIA','DUPLICADOR','NUMERO PUERTA']], how='left', on="NIS_RAD")

df_superBBDD5_BDI = df_superBBDD5[df_superBBDD5['BBDD'] == 'BDI' ]
df_superBBDD5_OSF = df_superBBDD5[df_superBBDD5['BBDD'] == 'OSF' ]
df_superBBDD5_BDIOSF = df_superBBDD5[df_superBBDD5['BBDD'] == 'BDI/OSF' ]

df_superBBDD5 = df_superBBDD4[['Periodo','BBDD','Criterios','Val_NIS_RAD','Val_MUNIC','NRO_ACOMETIDA','TRANSFORMADOR','TIP_ASOC',
'CAPTURADO','Propiedad Transformador','Sector (Mto.)-15','NIC','NIS_RAD','NIS_RAD_NEW','NIF_x','TIP_ASOC','PAN_NOMBRE_FINCA','ESTADO CLI','ESTADO TRAFO',
'DEPARTAMENTO','MUNICIPIO','CORREGIMIENTO','BARRIO','F_FACT_SGC','NOMBRE_CLIENTE','DIRECCION_OSF','DIRECCION_SUMINISTRO','TIPO VIA O CALLE','CALLE O VIA',
'DUPLICADOR','NUMERO PUERTA','DIRECCION_REFERENCIA','SERIE_ACTUAL','SERIE_ANTERIOR','MARCA','DESCRIPCION_FINCA','¿NIS DUPLICADO EN MED?','QR DEPURADO',
'¿NIC DUPLICADO EN QR?','¿NIS DUPLICADO EN QR?']]

# =============================================================================
# TIPO DE DATOS COLUMNAS
# =============================================================================
print(" ")
print("NORMALIZANDO LOS TIPOS DE DATOS")
df_superBBDD4["Periodo"] = str(fecha)
# df_superBBDD4["NRO_ACOMETIDA"] = df_superBBDD4["NRO_ACOMETIDA"].astype('int64')
df_superBBDD4["NIC"] = df_superBBDD4["NIC"].astype('int64')
df_superBBDD4["NIS_RAD"] = df_superBBDD4["NIS_RAD"].astype('int64')
df_superBBDD4["Periodo"] = df_superBBDD4["Periodo"].astype(str)
df_superBBDD4["QR DEPURADO"].fillna(0, inplace=True)
df_superBBDD4["QR DEPURADO"] = df_superBBDD4["QR DEPURADO"].astype('int64')
# df_superBBDD4["NUMERO PUERTA"] = df_superBBDD4["NUMERO PUERTA"].astype(str)

# =============================================================================
# GENERACION DE REPORTES
# =============================================================================
print(" ")
print("GENERANDO REPORTE...")
nombrearchivo="SuperBBDD_Clientes_"+str(fecha)+'.csv'
df_superBBDD4.to_csv(path_or_buf=nombrearchivo, sep=';', index=False)
print("REPORTE LISTO")
print(" ")
# tiempo de cargue
a = time.time() - t0
tiempoEjecucion = '{0:.2f}'.format(a)
tiempoEjecucion = float(tiempoEjecucion) - float(tiempoCargue)
print("El tiempo de ejecucion es: " + str(tiempoEjecucion) + " seg")

# =============================================================================
# 
# =============================================================================
df_superBBDD4 = pd.read_csv('SuperBBDD_Clientes_23 ENE.csv', encoding="ANSI", delimiter=";", on_bad_lines="skip",low_memory=False)



# =============================================================================
# adicional
# =============================================================================
df_superBBDD4["DEPARTAMENTO_CLI"].fillna(0, inplace=True)
df_superBBDD4.loc[:,"DEPARTAMENTO_CLI"] = df_superBBDD4.loc[:,"DEPARTAMENTO_CLI"].astype(str).apply(\
                str.replace,args=('LA GUAJIRA', 'GUAJIRA')) #remplazamos
df_superBBDD4.loc[:,"DEPARTAMENTO"] = df_superBBDD4.loc[:,"DEPARTAMENTO"].astype(str).apply(\
                str.replace,args=('Guajira', 'GUAJIRA')) #remplazamos

df_superBBDD4.loc[:,"DEPARTAMENTO_CLI"] = df_superBBDD4.loc[:,"DEPARTAMENTO_CLI"].astype(str).apply(\
                str.replace,args=('ATLÃƒÆ’Ã‚Æ’Ãƒâ€šÃ‚ÂNTICO', 'ATLANTICO')) #remplazamos
    
departamentos = df_superBBDD4['DEPARTAMENTO_CLI']
departamentos.drop_duplicates(keep='first', inplace=True)

df_ATLANTICO = df_superBBDD4[df_superBBDD4['DEPARTAMENTO_CLI'] == "ATLANTICO"]
df_ATLANTICO.to_csv(path_or_buf='clientes_atlantico.csv', sep=';', index=False)

df_guajira = df_superBBDD4[df_superBBDD4['DEPARTAMENTO_CLI'] == "LA GUAJIRA"]
df_guajira.to_csv(path_or_buf='clientes_guajira.csv', sep=';', index=False)

df_magd = df_superBBDD4[df_superBBDD4['DEPARTAMENTO_CLI'] == "MAGDALENA"]
df_magd.to_csv(path_or_buf='clientes_magdalena.csv', sep=';', index=False)

print("SUMINISTROS MLU cargando...... ")
df_sumMLU_1 = pd.read_excel('sumMLU.xlsx', sheet_name='parte 1')
print("Parte 1 cargada...")
df_sumMLU_2 = pd.read_excel('sumMLU.xlsx', sheet_name='parte 2')
print("Parte 2 cargada...")
df_sumMLU = pd.concat([df_sumMLU_1,df_sumMLU_2],axis=0)
df_sumMLU.reset_index(inplace=True) #resetar index
df_sumMLU = df_sumMLU.drop(columns = 'index') #elimina columna index
print("SUMINISTROS MLU Cargada Completamente")

print("Informe de clientes MLU cargando...... ")
df_clienteMLU = pd.read_csv('C:/Users/P568/Desktop/informes/InformeclientesMLU.csv', encoding="ANSI", delimiter=";", on_bad_lines="skip",low_memory=False)
x = df_clienteMLU.head(n=10)
print("Parte 1/1 cargada...")
#Eliminar duplicados equipo ruta id
df_clienteMLU = df_clienteMLU.drop_duplicates(subset=['EquipoRutaId']) 

print("clientes magdalena cargando...... ")
df_magd = pd.read_csv('clientes_magdalena.csv', encoding="ANSI", delimiter=";", on_bad_lines="skip",low_memory=False)
aa = df_clienteMLU.head(n=10)
print("Parte 1/1 cargada...")



copiarColumnna("Periodo", 0, df_magd)
copiarColumnna("BBDD", 1, df_magd)
copiarColumnna("NIC", 2, df_magd)
copiarColumnna("NIS_RAD", 3, df_magd)
copiarColumnna("SERIE_ACTUAL", 4, df_magd)
copiarColumnna("SERIE_ANTERIOR", 5, df_magd)
copiarColumnna("QR DEPURADO", 6, df_magd)
copiarColumnna("NOMBRE_CLIENTE", 7, df_magd)
copiarColumnna("LOCALIZACION", 8, df_magd)
copiarColumnna("DIRECCION_OSF", 9, df_magd)

df_magd.reset_index(inplace=True) #resetar index
df_magd = df_magd.drop(columns = 'index') #elimina columna index

df_sumMLU["EquipoRutaId"] = df_sumMLU["EquipoRutaId"].astype(str) 
df_clienteMLU["EquipoRutaId"] = df_clienteMLU["EquipoRutaId"].astype(str) 

df_sumMLU1 = df_sumMLU.merge(df_clienteMLU[["EquipoRutaId",'Estado del sticker','Estado del número del medidor']], how='left', on="EquipoRutaId")

df_magdNic0 = df_magd[df_magd['NIC'] == 0]
df_magd.loc[df_magdNic0.index,"NIC"] = "CERO"

clientesMagd = df_magd.merge(df_sumMLU1[["NIC",'NIS_RAD','Estado del número del medidor','Estado del sticker','INSTALACIO','Ruta','CODIGO']], how='left', on="NIS_RAD")
clientesMagd["val_duplicados_NIS_RAD"] = clientesMagd.duplicated(subset=["NIS_RAD"], keep=False)
clientesMagd.fillna(0, inplace=True)
clientesMagd_dup_NIC = clientesMagd[clientesMagd['val_duplicados_NIS_RAD'] == True ]

clientesMagd_dup_NIC = clientesMagd_dup_NIC[clientesMagd_dup_NIC['CODIGO'] != 0  ]
clientesMagd_dup_NIC['CODIGO'] = clientesMagd_dup_NIC['CODIGO'].astype(int)

clientesMagd['CODIGO'] = clientesMagd['CODIGO'].astype(int)
Sel = clientesMagd[clientesMagd['CODIGO'] == 0  ]
clientesMagd.loc[Sel.index,"val_NIS_RAD"] = "NO"

Sel = clientesMagd[clientesMagd['CODIGO'] != 0  ]
clientesMagd.loc[Sel.index,"val_NIS_RAD"] = "SI"

# df_sumMLU1['Estado del número del medidor'].fillna(0, inplace=True)
# clientesMagd = clientesMagd[clientesMagd['Estado del número del medidor'] == 0]
# df_magd.loc[df_magdNic0.index,"NIC"] = "CERO"
clientesMagd['SERIE_ACTUAL'] = clientesMagd['SERIE_ACTUAL'].astype(str)
clientesMagd['SERIE_ANTERIOR'] = clientesMagd['SERIE_ANTERIOR'].astype(str)

sel = clientesMagd[clientesMagd['SERIE_ACTUAL'] == "0"]
sel1 = sel[sel['SERIE_ANTERIOR'] == "0"]
clientesMagd.loc[sel1.index,"val_MEDIDOR"] = "N/A"
clientesMagd.loc[sel.index,"SERIE_ACTUAL"] = "N/A"
clientesMagd.loc[sel1.index,"SERIE_ANTERIOR"] = "N/A"

df_sumMLU1['Estado del número del medidor'] = df_sumMLU1['Estado del número del medidor'].astype(int, errors='ignore')
x = df_sumMLU1[df_sumMLU1['Estado del número del medidor'] == "A360445289"]
df_sumMLU1.loc[x.index,"Estado del número del medidor"] = 0

clientesMagd1 = clientesMagd.merge(df_sumMLU1[["NIC",'NIS_RAD','Estado del número del medidor','Estado del sticker','INSTALACIO','Ruta','CODIGO']], how='left', left_on=('SERIE_ACTUAL'), right_on=('Estado del número del medidor'))

dfPrueba = df_sumMLU1[[ df_sumMLU1['NIC'] == 6811498,'NIS_RAD']]
dfPrueba['NIC'] =dfPrueba['NIC'].astype(int)
dfPrueba.head(n=10)
dfPrueba.fillna(0, inplace=True)

clientesMagd.to_csv(path_or_buf='clientesMagdalena.csv', sep=';', index=False)

departamentos = df_sumMLU1['TERRITORIO']
departamentos.drop_duplicates(keep='first', inplace=True)
clientesMluMagdalena = df_sumMLU1[df_sumMLU1['TERRITORIO'] == "03. MAGDALENA"]
clientesMluMagdalena.to_csv(path_or_buf='clientesMluMagdalena.csv', sep=';', index=False)


df = df_sumMLU1.reset_index()
df1 = df[df['index'] <= 999999] #filtramos ID_BDI en cero
df2 = df[df['index'] > 999999] #filtramos ID_BDI en cero

df1 = df1.drop(columns = 'index') #elimina columna index
df2 = df2.drop(columns = 'index') #elimina columna index


#imprimimos el df en un excel
df1.to_excel('sumMLU_1-nuevo.xlsx', sheet_name='parte 1', index = False)
df2.to_excel('sumMLU_2-nuevo.xlsx', sheet_name='parte 2', index = False)