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
df_ConsultaDinamica = pd.read_excel('Pr01 h.2 - Certificación_Cód_BDI_May.22(inicio).xlsx', sheet_name="Pr01 h3", header=5)
df_ConsultaDinamica = df_ConsultaDinamica.drop(columns = 'Unnamed: 0')

# CARGUE DE "Pr01 h4"
df_analisisEspacial = pd.read_excel('Pr01 h.2 - Certificación_Cód_BDI_May.22(inicio).xlsx', sheet_name="Pr01 h4", header=4)
df_analisisEspacial = df_analisisEspacial.drop(columns = 'Unnamed: 0')

# CARGUE DE "Pr01 h5"
df_DistanciaEntreCod = pd.read_excel('Pr01 h.2 - Certificación_Cód_BDI_May.22(inicio).xlsx', sheet_name="Pr01 h5", header=4)
df_DistanciaEntreCod = df_DistanciaEntreCod.drop(columns = 'Unnamed: 0')

# tiempo de cargue
tiempoCargue = '{0:.2f}'.format(time.time() - t0)
print('BBDD cargadas en ' + tiempoCargue + ' seg')

# CANT BBDD
nTxMat = len(df_txMatriculados)
nConsultaDinamica = len(df_ConsultaDinamica)
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

# Pr01 h2 - txMatriculados
# Análisis Duplicidad
df_txMatriculados.insert(7,"Análisis Duplicidad",None)
df_txMatriculados["Análisis Duplicidad"] = df_txMatriculados.duplicated(subset='Nuevo Cód.', keep=False).astype(str)
df_txMatriculados["Análisis Duplicidad"].replace('False', 'NO', inplace=True)
df_txMatriculados["Análisis Duplicidad"].replace('True', 'SI', inplace=True)
df_txMatriculados["Análisis Duplicidad"].fillna('N/A', inplace=True)

#TX en BDI [Si/No]
df_txMatriculados.insert(8,"TX en BDI [Si/No]",None)
df_txMatriculados["TX en BDI [Si/No]"] = CertTxMatBdi["Nuevo Cód."].isin(df_ConsultaDinamica['CODIGO']).astype(str)
df_txMatriculados["TX en BDI [Si/No]"].replace('False', 'NO', inplace=True)
df_txMatriculados["TX en BDI [Si/No]"].replace('True', 'SI', inplace=True)
df_txMatriculados["TX en BDI [Si/No]"].fillna('N/A', inplace=True)

# Pr01 h4 - analisisEspacial
# CODIGO Censo
df_analisisEspacial.insert(0,"CODIGO Censo",None)
df_analisisEspacial = df_analisisEspacial.merge(df_txMatriculados[['OBJECTID *', 'Nuevo Cód.']], left_on='IN_FID', right_on="OBJECTID *" ,how='left'  ,suffixes=('', '_r'))
df_analisisEspacial["CODIGO Censo"] = df_analisisEspacial['Nuevo Cód.']
del df_analisisEspacial['Nuevo Cód.']
del df_analisisEspacial['OBJECTID *_r']

# CODIGO BDI
df_analisisEspacial.insert(1,"CODIGO BDI",None)
df_analisisEspacial = df_analisisEspacial.merge(df_ConsultaDinamica[['FID', 'CODIGO']], left_on='NEAR_FID', right_on="FID" ,how='left'  ,suffixes=('', '_r'))
df_analisisEspacial["CODIGO BDI"] = df_analisisEspacial['CODIGO']
del df_analisisEspacial['CODIGO']
del df_analisisEspacial['FID']

# Pr01 h2 - txMatriculados
# Mas Cercano
df_txMatriculados.insert(9,"Mas Cercano",None)
df_txMatriculados = df_txMatriculados.merge(df_analisisEspacial[["CODIGO Censo", "CODIGO BDI"]], left_on="Nuevo Cód.", right_on='CODIGO Censo' ,how='left',suffixes=('', '_r'))
df_txMatriculados = df_txMatriculados.drop_duplicates(subset=['OBJECTID *'])
df_txMatriculados["Mas Cercano"] = df_txMatriculados["Nuevo Cód."] == df_txMatriculados["CODIGO BDI"]
df_txMatriculados["Mas Cercano"].replace(False, 'NO', inplace=True)
df_txMatriculados["Mas Cercano"].replace(True, 'SI', inplace=True)
df_txMatriculados["Mas Cercano"].fillna('N/A', inplace=True)
del df_txMatriculados["CODIGO Censo"]
del df_txMatriculados["CODIGO BDI"]

# Cód. BDI Certificado
df_txMatriculados.insert(10,"Cód. BDI Certificado",None)

# Distancia entre Cód_Censo & Cód_BDI
df_txMatriculados.insert(11,"Distancia entre Cód_Censo & Cód_BDI",None)
df_txMatriculados = df_txMatriculados.merge(df_DistanciaEntreCod[["Nuevo_Cód", "Shape_Length"]], left_on="Nuevo Cód.", right_on='Nuevo_Cód' ,how='left',suffixes=('', '_r'))
df_txMatriculados["Distancia entre Cód_Censo & Cód_BDI"] = df_txMatriculados["Shape_Length"]
del df_txMatriculados["Shape_Length"]
del df_txMatriculados["Nuevo_Cód"]

# MATRICULA_ANT [Si/No]
df_txMatriculados.insert(14,"MATRICULA_ANT [Si/No]",None)
Sel = df_txMatriculados[df_txMatriculados['MATRICULA_ANT'] == "ILEGIBLE"].index 
df_txMatriculados.loc[Sel,"MATRICULA_ANT [Si/No]"] =  "NO"
Sel = df_txMatriculados[df_txMatriculados['MATRICULA_ANT'] == "IELGIBLE"].index 
df_txMatriculados.loc[Sel,"MATRICULA_ANT [Si/No]"] =  "NO"
Sel = df_txMatriculados[df_txMatriculados['MATRICULA_ANT'] == "ILIGIBLE"].index 
df_txMatriculados.loc[Sel,"MATRICULA_ANT [Si/No]"] =  "NO"
Sel = df_txMatriculados[df_txMatriculados['MATRICULA_ANT'] == "ILLEGIBLE"].index 
df_txMatriculados.loc[Sel,"MATRICULA_ANT [Si/No]"] =  "NO"
Sel = df_txMatriculados[df_txMatriculados['MATRICULA_ANT'] == "INEXISTENTE"].index 
df_txMatriculados.loc[Sel,"MATRICULA_ANT [Si/No]"] =  "NO"
Sel = df_txMatriculados[df_txMatriculados['MATRICULA_ANT'] == None ].index 
df_txMatriculados.loc[Sel,"MATRICULA_ANT [Si/No]"] =  "NO"
df_txMatriculados["MATRICULA_ANT [Si/No]"].fillna('SI', inplace=True)

# MATRICULA_ANT_CD
df_txMatriculados.insert(16,"MATRICULA_ANT_CD",None)
df_txMatriculados = df_txMatriculados.merge(df_ConsultaDinamica[["CODIGO", "MATRICULA_"]], left_on="Nuevo Cód.", right_on='CODIGO' ,how='left',suffixes=('', '_r'))
df_txMatriculados = df_txMatriculados.drop_duplicates(subset=['OBJECTID *'])
df_txMatriculados["MATRICULA_ANT_CD"] = df_txMatriculados["MATRICULA_"].astype(str).str.upper()
del df_txMatriculados["MATRICULA_"]
del df_txMatriculados["CODIGO_r"]

# Val: MATRICULA_ANT
df_txMatriculados.insert(17,"Val: MATRICULA_ANT",None)
df_txMatriculados["MATRICULA_ANT"] = df_txMatriculados["MATRICULA_ANT"].astype(str).str.upper()
df_txMatriculados["Val: MATRICULA_ANT"] = df_txMatriculados["MATRICULA_ANT"] == df_txMatriculados["MATRICULA_ANT_CD"]
df_txMatriculados["Val: MATRICULA_ANT"].replace(False, 'NO', inplace=True)
df_txMatriculados["Val: MATRICULA_ANT"].replace(True, 'SI', inplace=True)
df_txMatriculados["Val: MATRICULA_ANT"].fillna('N/A', inplace=True)

# POTENCIA_NOMINAL
df_txMatriculados.insert(19,"POTENCIA_NOMINAL_CD",None)
df_txMatriculados = df_txMatriculados.merge(df_ConsultaDinamica[["CODIGO", "POTENCIA_N"]], left_on="Nuevo Cód.", right_on='CODIGO' ,how='left',suffixes=('', '_r'))
df_txMatriculados = df_txMatriculados.drop_duplicates(subset=['OBJECTID *'])
df_txMatriculados["POTENCIA_NOMINAL_CD"] = df_txMatriculados["POTENCIA_N"]
del df_txMatriculados["POTENCIA_N"]
del df_txMatriculados["CODIGO_r"]

# Val: POTENCIA_NOMINAL
df_txMatriculados.insert(20,"Val: POTENCIA_NOMINAL",None)
df_txMatriculados["Val: POTENCIA_NOMINAL"] = df_txMatriculados["POTENCIA_NOMINAL"] == df_txMatriculados["POTENCIA_NOMINAL_CD"]
df_txMatriculados["Val: POTENCIA_NOMINAL"].replace(False, 'NO', inplace=True)
df_txMatriculados["Val: POTENCIA_NOMINAL"].replace(True, 'SI', inplace=True)
df_txMatriculados["Val: POTENCIA_NOMINAL"].fillna('N/A', inplace=True)

# Cód. BDI Certificado
df_txMatriculados["Cód. BDI Certificado"].fillna(0, inplace=True)
    ##
sel = df_txMatriculados[df_txMatriculados["Tipo_Cod_Nuevo"] == "BDI"]
sel = sel[sel["TX en BDI [Si/No]"] == "SI"]

    ## Mat Igual: Si; Mas Cerc: Si
selMatSi =  sel[sel["Val: MATRICULA_ANT"] == "SI"]
selMatSiCercSi = selMatSi[selMatSi["Mas Cercano"]   == "SI"]
selMatSiCercSi = selMatSiCercSi[selMatSiCercSi["Cód. BDI Certificado"] == 0]
indexImp = selMatSiCercSi.index
df_txMatriculados.loc[indexImp,"Cód. BDI Certificado"] =  "Mat Igual: Si; Mas Cerc: Si"

    ## Mat Igual: Si; Mas Cerc: No; Dist_Entre_Cód's: <= 150 m
selMatSiCercNo = selMatSi[selMatSi["Mas Cercano"] == "NO"]
selMatSiCercNo_150 = selMatSiCercNo[selMatSiCercNo["Distancia entre Cód_Censo & Cód_BDI"] <= 150]
selMatSiCercNo_150 = selMatSiCercNo_150[selMatSiCercNo_150["Cód. BDI Certificado"] == 0]
indexImp = selMatSiCercNo_150.index
df_txMatriculados.loc[indexImp,"Cód. BDI Certificado"] =  "Mat Igual: Si; Mas Cerc: No; Dist_Entre_Cód's: <= 150 m"
    
    ## Mat Igual: No; Mas Cerc: Si; Dist_Entre_Cód's: <= 30 m
selMatNo = sel[sel["Val: MATRICULA_ANT"] == "NO"]
selMatNo = selMatNo[selMatNo["MATRICULA_ANT [Si/No]"] == "SI"]
selMatNoCercSi = selMatNo[selMatNo["Mas Cercano"] == "SI"]
selMatNoCercSi_30 = selMatNoCercSi[selMatNoCercSi["Distancia entre Cód_Censo & Cód_BDI"] <= 30]
selMatNoCercSi_30 = selMatNoCercSi_30[selMatNoCercSi_30["Cód. BDI Certificado"] == 0]
indexImp = selMatNoCercSi_30.index
df_txMatriculados.loc[indexImp,"Cód. BDI Certificado"] =  "Mat Igual: No; Mas Cerc: Si; Dist_Entre_Cód's: <= 30 m"

    ## Mat Igual: N/A; Mas Cerc: Si; Pot Igual: Si
selMatNo = sel[sel["Val: MATRICULA_ANT"] == "NO"]
selMatNA = selMatNo[selMatNo["MATRICULA_ANT [Si/No]"] == "NO"]
selMatNACercSi = selMatNA[selMatNA["Mas Cercano"] == "SI"]
selMatNACercSiPotSi = selMatNACercSi[selMatNACercSi["Val: POTENCIA_NOMINAL"] == "SI"]
selMatNACercSiPotSi = selMatNACercSiPotSi[selMatNACercSiPotSi["Cód. BDI Certificado"] == 0]
indexImp = selMatNACercSiPotSi.index
df_txMatriculados.loc[indexImp,"Cód. BDI Certificado"] =  "Mat Igual: N/A; Mas Cerc: Si; Pot Igual: Si"

    ## Mat Igual: N/A; Mas Cerc: No; Pot Igual: Si; Dist_Entre_Cód's: <= 30 m
selMatNACercNo = selMatNA[selMatNA["Mas Cercano"] == "NO"]
selMatNACercNoPotSi = selMatNACercNo[selMatNACercNo["Val: POTENCIA_NOMINAL"] == "SI"]
selMatNACercNoPotSi_30 = selMatNACercNoPotSi[selMatNACercNoPotSi["Distancia entre Cód_Censo & Cód_BDI"] <= 30]
selMatNACercNoPotSi_30 = selMatNACercNoPotSi_30[selMatNACercNoPotSi_30["Cód. BDI Certificado"] == 0]
indexImp = selMatNACercNoPotSi_30.index
df_txMatriculados.loc[indexImp,"Cód. BDI Certificado"] =  "Mat Igual: N/A; Mas Cerc: No; Pot Igual: Si; Dist_Entre_Cód's: <= 30 m"




tiempoEjecucion = '{0:.2f}'.format(time.time() - t0)
print('Reporte generado en ' + tiempoEjecucion + ' seg')










