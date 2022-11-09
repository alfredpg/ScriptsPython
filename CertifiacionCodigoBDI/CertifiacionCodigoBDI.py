# import dask
import pandas as pd
# import dask.dataframe as dd
# import numpy as np
# from pandas import ExcelWriter
# from fConsolidarClientes import DistanciaCoord
# import tkinter as tk
# from tkinter import filedialog
# from tkinter import messagebox
# from xlwt import Workbook
# import xlwt
import time
# import sys

# import warnings


# cargamos informe como dataframe
# file_path = filedialog.askopenfilename(defaultextension=".xls")
# if file_path is None:
#     # quit()
#     sys.exit()

t0 = time.time()
# CARGUE BBDD ISES
df_txMatriculados = pd.read_excel('Pr01 h.2 - Certificación_Cód_BDI_May.22(inicio).xlsx', sheet_name="Pr01 h2", header=9)
df_txMatriculados = df_txMatriculados.drop(columns = 'Unnamed: 0') 

# CARGUE DE BBDD BDI
df_txBDA = pd.read_excel('Pr01 h.2 - Certificación_Cód_BDI_May.22(inicio).xlsx', sheet_name="Pr01 h3", header=5)
df_txBDA = df_txBDA.drop(columns = 'Unnamed: 0')

# CARGUE DE "Pr01 h4"
df_analisisEspacial = pd.read_excel('Pr01 h.2 - Certificación_Cód_BDI_May.22(inicio).xlsx', sheet_name="Pr01 h4", header=4)
df_analisisEspacial = df_analisisEspacial.drop(columns = 'Unnamed: 0')

# CARGUE DE "Pr01 h5"
df_DistanciaEntreCod = pd.read_excel('Pr01 h.2 - Certificación_Cód_BDI_May.22(inicio).xlsx', sheet_name="Pr01 h5", header=4)
df_DistanciaEntreCod = df_DistanciaEntreCod.drop(columns = 'Unnamed: 0')

# CANT BBDD
nTxMat = len(df_txMatriculados)
nTxBda = len(df_txBDA)
nAnalisisEspacial = len(df_analisisEspacial)
nDistanciaEntreCod = len(df_DistanciaEntreCod)


#TX MATRICULADOS
txMatBdi = df_txMatriculados[df_txMatriculados['Tipo_Cod'] == 'BDI' ]
ntxMatBdi = len(txMatBdi)

txMatElectro = df_txMatriculados[df_txMatriculados['Tipo_Cod'] == 'Electro' ]
ntxMatElectro = len(txMatElectro)

txMatIses = df_txMatriculados[df_txMatriculados['Tipo_Cod'] == 'Ises' ]
ntxMatIses = len(txMatIses)


#TX MATRICULADOS CERTIFICADO
CertTxMatBdi = df_txMatriculados[df_txMatriculados['Tipo_Cod_Nuevo'] == 'BDI' ]
nCertTxMatBdi = len(CertTxMatBdi)

CertTxMatElectro = df_txMatriculados[df_txMatriculados['Tipo_Cod_Nuevo'] == 'Electro' ]
nCertTxMatElectro = len(CertTxMatElectro)

CertTxMatIses = df_txMatriculados[df_txMatriculados['Tipo_Cod_Nuevo'] == 'Ises' ]
nCertTxMatIses = len(CertTxMatIses)


# DIFERENCIAS Mat VS MatCert
difMatBdi = nCertTxMatBdi - ntxMatBdi
difMatElectro = nCertTxMatElectro - ntxMatElectro
difMatIses = nCertTxMatIses - ntxMatIses

# Análisis Duplicidad
df_txMatriculados.insert(7,"Análisis Duplicidad",None)
df_txMatriculados["Análisis Duplicidad"] = df_txMatriculados.duplicated(subset='Nuevo Cód.', keep=False).astype(str)
df_txMatriculados["Análisis Duplicidad"].replace('False', 'NO', inplace=True)
df_txMatriculados["Análisis Duplicidad"].replace('True', 'SI', inplace=True)
df_txMatriculados["Análisis Duplicidad"].fillna('N/A', inplace=True)


#TX en BDI [Si/No]
df_txMatriculados.insert(8,"TX en BDI [Si/No]",None)
#df = CertTxMatBdi.merge(df_txBDA['CODIGO'], left_on='Nuevo Cód.', right_on='CODIGO', how='left')
df_txMatriculados["TX en BDI [Si/No]"] = CertTxMatBdi["Nuevo Cód."].isin(df_txBDA['CODIGO']).astype(str)
df_txMatriculados["TX en BDI [Si/No]"].replace('False', 'NO', inplace=True)
df_txMatriculados["TX en BDI [Si/No]"].replace('True', 'SI', inplace=True)
df_txMatriculados["TX en BDI [Si/No]"].fillna('N/A', inplace=True)

# Pr01 h4
# CODIGO Censo
df_analisisEspacial.insert(0,"CODIGO Censo",None)
#dfSel = df_txMatriculados[df_txMatriculados['OBJECTID *', 'CODIGO']]
df_analisisEspacial = df_analisisEspacial.merge(df_txMatriculados[['OBJECTID *', 'CODIGO']], on='OBJECTID *' ,how='left')
df_analisisEspacial["CODIGO Censo"] = df_analisisEspacial['CODIGO']
del df_analisisEspacial['CODIGO']






elapsed_time = '{0:.2f}'.format(time.time() - t0)
print('Reporte generado en ' + elapsed_time + ' seg')










