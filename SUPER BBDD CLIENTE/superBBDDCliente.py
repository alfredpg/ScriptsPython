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
print("BBDD 530 cargando...... ")
df_530_1 = pd.read_excel('BBDD Cierre DICIEMBRE.xlsx', sheet_name='530-1')
print("Parte 1 cargada...")
df_530_2 = pd.read_excel('BBDD Cierre DICIEMBRE.xlsx', sheet_name='530-2')
print("Parte 2 cargada...")
df_530 = pd.concat([df_530_1,df_530_2],axis=0)
df_530.reset_index(inplace=True) #resetar index
df_530 = df_530.drop(columns = 'index') #elimina columna index
print("BBDD 530 Cargada Completamente")
print(" ")
print("BBDD medidores cargando...... ")
df_med_1 = pd.read_excel('BBDD Cierre DICIEMBRE.xlsx', sheet_name='MED 1')
print("Parte 1 cargada...")
df_med_2 = pd.read_excel('BBDD Cierre DICIEMBRE.xlsx', sheet_name='MED 2')
print("Parte 2 cargada...")
df_med = pd.concat([df_med_1,df_med_2],axis=0)
df_med.reset_index(inplace=True) #resetar index
df_med = df_med.drop(columns = 'index') #elimina columna index
print("BBDD mededidores Cargada")
print(" ")
print("BBDD direcciones cargando...... ")
df_dir_1 = pd.read_excel('BBDD Cierre DICIEMBRE.xlsx', sheet_name='DIR 1')
print("Parte 1 cargada...")
df_dir_2 = pd.read_excel('BBDD Cierre DICIEMBRE.xlsx', sheet_name='DIR 2')
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
fecha = "22 dic"

# =============================================================================
# VALIDAR REGISTROS DUPLICADOS Y ERRONEOS EN BBDD
# =============================================================================

#validar duplicados y errores en 530
print(" ")
print("Validando 530...")
df_530["ACOMETIDA"].fillna("NN", inplace=True) #cambiar vacias por 0 en ACOMETIDA
df_530Error = df_530[df_530["ACOMETIDA"] == "NN" ] #seleccionar ACOMETIDAS en 0
df_530.drop(df_530Error.index , inplace=True) #eliminar seleccion pasada por que son registros erroneos
print(str(len(df_530Error)) +" registros erroneos eliminados 530")
    #duplicados
df_530["val_duplicados"] = df_530.duplicated(subset=["NIC"], keep=False)
df_530_dup = df_530[df_530['val_duplicados'] == True ]
df_530_ok = df_530[df_530['val_duplicados'] == False ]
print("Se encontraron "+str(len(df_530_dup)) +" NIC´S duplicados en 530")
print("Se encontraron "+str(len(df_530_ok)) +" NIC´S OK en 530")

#validar duplicados y errores en osf
print(" ")
print("validado OSF...")
df_osf["val_duplicados"] = df_osf.duplicated(subset=["NIC"], keep=False)
df_osf_dup = df_osf[df_osf['val_duplicados'] == True ]
df_osf_ok = df_osf[df_osf['val_duplicados'] == False ]
print("Se encontraron "+str(len(df_osf_dup)) +" NIC´S duplicados en OSF")
print("Se encontraron "+str(len(df_osf_ok)) +" NIC´S OK en OSF")

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
print(" ")
print("validado DIRECCIONES...")
df_dir["val_duplicados"] = df_dir.duplicated(subset=["NIC"], keep=False)
df_dir_dup = df_dir[df_dir['val_duplicados'] == True ]
df_dir_ok = df_dir[df_dir['val_duplicados'] == False ]
print("Se encontraron "+str(len(df_dir_dup)) +" NIC´S duplicados en DIRECCIONES")
print("Se encontraron "+str(len(df_dir_ok)) +" NIC´S OK en DIRECCIONES")

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
df_530_ok["NIC"].fillna(0, inplace=True)
df_osf_ok["NIC"].fillna(0, inplace=True)
df_530_ok["NIC"] = df_530_ok["NIC"].astype("int64") 
df_osf_ok["NIC"] = df_osf_ok["NIC"].astype("int64") 
#unir 530 y OSF por NIC conservando la diferencia de los dos
df_superBBDD = df_530_ok.merge(df_osf_ok, how='outer', on='NIC')
df_superBBDD["NIS_RAD_y"].fillna(0, inplace=True) #cambiar vacias por 0

#columnas de verificacion y seguimiento
df_superBBDD.insert(0,"Periodo","-") #Insertar columna periodo
df_superBBDD.insert(1,"BBDD",0) #insertar columna BBDD
df_superBBDD.insert(2,"Criterios",0) #insertar columna Criterios

#crear df de los que no traen NIS_RAD por que no cruzaron con OSF
BDI = df_superBBDD[df_superBBDD['NIS_RAD_y'] == 0]
df_superBBDD.loc[BDI.index,"BBDD"] = "BDI"  #colocar BDI en columna BBDD
df_superBBDD.loc[BDI.index,"Criterios"] = "N/A" #No aplican para criterio

#crear df de los que ACOMETIDA es cero por que esos son los que no estaban en BDI son meramente OSF
df_superBBDD["ACOMETIDA"].fillna(0, inplace=True) #cambiar vacias por 0 en ACOMETIDA
OSF = df_superBBDD[df_superBBDD['ACOMETIDA'] == 0 ] #filtrar 0
df_superBBDD.loc[OSF.index,"BBDD"] = "OSF" #colocar OSF en BBDD
    #traer los datos del cruce de OSF
df_superBBDD.loc[OSF.index,"Criterios"] = "N/A"
df_superBBDD.loc[OSF.index,"CODIGO_TRAFO"] = df_superBBDD.loc[:,"CODIGO_TRAFO_MB"]
df_superBBDD.loc[OSF.index,"Sector (Mto.)-15"] = df_superBBDD.loc[:,"TERRITORIO"]
df_superBBDD.loc[OSF.index,"NIS_RAD_x"] = df_superBBDD.loc[:,"NIS_RAD_y"]
df_superBBDD.loc[OSF.index,"DEPARTAMENTO"] = df_superBBDD.loc[:,"NOM_DEPTO"]
df_superBBDD.loc[OSF.index,"MUNICIPIO"] = df_superBBDD.loc[:,"NOM_MUNIC"]
df_superBBDD.loc[OSF.index,"CORREGIMIENTO"] = df_superBBDD.loc[:,"NOM_CORREG"]
df_superBBDD.loc[OSF.index,"DIRECCION_REFERENCIA"] = df_superBBDD.loc[:,"NOM_LOCAL"]
df_superBBDD.loc[OSF.index,"NOMBRE_CLIENTE"] = df_superBBDD.loc[:,"NOMBRE"]
df_superBBDD.loc[OSF.index,"DIRECCION_SUMINISTRO"] = df_superBBDD.loc[:,"DIRECCION"]

#crear df con los que NO cumplen las condiciones pasadas
BDIOSF = df_superBBDD[df_superBBDD['BBDD'] == 0 ] #filtrar 0
df_superBBDD.loc[BDIOSF.index,"BBDD"] = "BDI/OSF" #colocar BDI/OSF en BBDD
    #logica para los criterios
        # Val: NIS_RAD
df_superBBDD.insert(3,"Val_NIS_RAD","N/A")
df_superBBDD.loc[BDIOSF.index,"Val_NIS_RAD"] = BDIOSF["NIS_RAD_x"] == BDIOSF["NIS_RAD_y"]
df_superBBDD["Val_NIS_RAD"].replace(False, 'NO', inplace=True)
df_superBBDD["Val_NIS_RAD"].replace(True, 'SI', inplace=True)
df_superBBDD["Val_NIS_RAD"].fillna('N/A', inplace=True)

        # Val: MUNICIPIO
df_superBBDD.insert(4,"Val_MUNIC","N/A")
df_superBBDD.loc[BDIOSF.index,"Val_MUNIC"] = BDIOSF["MUNICIPIO"] == BDIOSF["NOM_MUNIC"]
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
df_superBBDD.loc[BDIOSF.index,"Criterios"] = "Nic: Si, Nis_R: "+df_superBBDD["Val_NIS_RAD"]+", Mun: "+df_superBBDD["Val_MUNIC"]

#eliminar las siguientes columnas
df_superBBDD = df_superBBDD.drop(columns=['val_duplicados_x','NIS_RAD_y',"NOMBRE",'TARIFA', 'COD_LOCAL','NOM_LOCAL',"NOM_MUNIC","NOM_CORREG",'NOM_DEPTO',
                                          'DESC_ASIGNACION','DESC_PROD_STATUS','LONGITUD','LATITUD','CSMO_ACTIVA','CODIGO_CIRCUITO','CODIGO_TRAFO_MB',
                                          'TERRITORIO','val_duplicados_y'])

df_superBBDD["Criterios"] = df_superBBDD["Criterios"].astype(str)
#renombrar columnas
df_superBBDD.rename(columns={'DIRECCION':'DIRECCION_OSF','NIS_RAD_x':'NIS_RAD' }, inplace=True)

# =============================================================================
# SUPERBBDD2 = SUPERBBDD + MEDIDORES
# =============================================================================
print(" ")
print("GENERANDO... SUPERBBDD2 = SUPERBBDD + MEDIDORES")
#Cruzar solo los nis presentes en SUPERBBDD
df_superBBDD2 = df_superBBDD.merge(df_med, how='left', on='NIS_RAD')
df_superBBDD2 = df_superBBDD2.drop(columns=['NRO_ACOMETIDA','NIF_y'])
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
df_superBBDD4_BDI_NIS_NA = df_superBBDD4_BDI[df_superBBDD4_BDI['¿NIC DUPLICADO EN QR?'] == 'N/A' ]
df_superBBDD4_BDI_NIS_OK = df_superBBDD4_BDI[df_superBBDD4_BDI['¿NIC DUPLICADO EN QR?'] == 'NO' ]
df_superBBDD4_BDI_NIS_DUP = df_superBBDD4_BDI[df_superBBDD4_BDI['¿NIC DUPLICADO EN QR?'] == 'SI' ]

df_superBBDD4_OSF = df_superBBDD4[df_superBBDD4['BBDD'] == 'OSF' ]
df_superBBDD4_OSF_NIS_NA = df_superBBDD4_OSF[df_superBBDD4_OSF['¿NIC DUPLICADO EN QR?'] == 'N/A' ]
df_superBBDD4_OSF_NIS_OK = df_superBBDD4_OSF[df_superBBDD4_OSF['¿NIC DUPLICADO EN QR?'] == 'NO' ]
df_superBBDD4_OSF_NIS_DUP = df_superBBDD4_OSF[df_superBBDD4_OSF['¿NIC DUPLICADO EN QR?'] == 'SI' ]

df_superBBDD4_BDIOSF = df_superBBDD4[df_superBBDD4['BBDD'] == 'BDI/OSF' ]
df_superBBDD4_BDIOSF_NIS_NA = df_superBBDD4_BDIOSF[df_superBBDD4_BDIOSF['¿NIC DUPLICADO EN QR?'] == 'N/A' ]
df_superBBDD4_BDIOSF_NIS_OK = df_superBBDD4_BDIOSF[df_superBBDD4_BDIOSF['¿NIC DUPLICADO EN QR?'] == 'NO' ]
df_superBBDD4_BDIOSF_NIS_DUP = df_superBBDD4_BDIOSF[df_superBBDD4_BDIOSF['¿NIC DUPLICADO EN QR?'] == 'SI' ]

# =============================================================================
# SUPERBBDD5 = SUPERBBDD4 + DIRECCIONES
# =============================================================================
print(" ")
print("GENERANDO... SUPERBBDD5 = SUPERBBDD4 + DIRECCIONES")
df_superBBDD5 = df_superBBDD4.merge(df_dir[["NIC",'TIPO VIA O CALLE','CALLE O VIA','DUPLICADOR','NUMERO PUERTA']], how='left', on="NIC")

df_superBBDD5 = df_superBBDD5[['Periodo','BBDD','Criterios','Val_NIS_RAD','Val_MUNIC','ACOMETIDA','CODIGO_TRAFO','CODIGO','MATRICULA_MT','Tipo Asoc TR',
'CAPTURADO','Propiedad Transformador','Sector (Mto.)-15','NIC','NIS_RAD','NIS_RAD_NEW','NIF_x','TIP_ASOC','PAN_NOMBRE_FINCA','ESTADO CLI','ESTADO TRAFO',
'DEPARTAMENTO','MUNICIPIO','CORREGIMIENTO','BARRIO','F_FACT_SGC','NOMBRE_CLIENTE','DIRECCION_OSF','DIRECCION_SUMINISTRO','TIPO VIA O CALLE','CALLE O VIA',
'DUPLICADOR','NUMERO PUERTA','DIRECCION_REFERENCIA','SERIE_ACTUAL','SERIE_ANTERIOR','MARCA','DESCRIPCION_FINCA','¿NIS DUPLICADO EN MED?','QR DEPURADO',
'¿NIC DUPLICADO EN QR?','¿NIS DUPLICADO EN QR?']]

# =============================================================================
# TIPO DE DATOS COLUMNAS
# =============================================================================
print(" ")
print("NORMALIZANDO LOS TIPOS DE DATOS")
df_superBBDD5["Periodo"] = str(fecha)
df_superBBDD5["ACOMETIDA"] = df_superBBDD5["ACOMETIDA"].astype('int64')
df_superBBDD5["NIC"] = df_superBBDD5["NIC"].astype('int64')
df_superBBDD5["Periodo"] = df_superBBDD5["Periodo"].astype(str)
df_superBBDD5["QR DEPURADO"].fillna(0, inplace=True)
df_superBBDD5["QR DEPURADO"] = df_superBBDD5["QR DEPURADO"].astype('int64')
df_superBBDD5["NUMERO PUERTA"] = df_superBBDD5["NUMERO PUERTA"].astype(str)

# =============================================================================
# GENERACION DE REPORTES
# =============================================================================
print(" ")
print("GENERANDO REPORTE...")
nombrearchivo="SuperBBDD_Clientes_"+str(fecha)+'.csv'
df_superBBDD5.to_csv(path_or_buf=nombrearchivo, sep=';', index=False)
print("REPORTE LISTO")
print(" ")
# tiempo de cargue
a = time.time() - t0
tiempoEjecucion = '{0:.2f}'.format(a)
tiempoEjecucion = float(tiempoEjecucion) - float(tiempoCargue)
print("El tiempo de ejecucion es: " + str(tiempoEjecucion) + " seg")

