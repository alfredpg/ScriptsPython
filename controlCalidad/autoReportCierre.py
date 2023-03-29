#import dask
import pandas as pd
#import dask.dataframe as dd
import numpy as np
from pandas import ExcelWriter
import tkinter as tk
from tkinter import filedialog
from tkinter import messagebox
#from xlwt import Workbook
#import xlwt
import time
import sys
import xlsxwriter
import os.path

import warnings
warnings.filterwarnings('ignore')
warnings.warn('DelftStack')
warnings.warn('Do not show this message')
print("No Warning Shown")

# =============================================================================
# Funciones
# =============================================================================
def copiarColumnna(columna,posicion,df):
    copia = str(columna) + "-"
    df.insert(posicion,copia,0)
    df[copia] = df[columna]

# =============================================================================
# 65204928,00000000000
# =============================================================================

warnings.filterwarnings('ignore')
warnings.warn('DelftStack')
warnings.warn('Do not show this message')

# cargamos informe como dataframe
file_path = filedialog.askopenfilename()
if file_path is None:
    # quit()
    sys.exit()

nombre, extension = os.path.splitext(file_path)

if extension == '.csv':
    df = pd.read_csv(file_path, encoding="ANSI", delimiter=";", on_bad_lines="skip",low_memory=False)
else:
    df = pd.read_excel(file_path, header=0)

t0 = time.time()

# sel = df[df['ID_BDI'] == "65204928,00000000000"]

#rellenar vacias por ceros
df.fillna(0, inplace=True)

df.insert(0,"CODIGO",0)
sel = df[df['ID_BDI'] == "trasformador fue desmontado yel sitio remodelado"]
df.loc[sel.index,"ID_BDI"] =  0
df['CODIGO'] = df['ID_BDI'].astype("int64").abs()
sel = df[df['CODELEME'] == "TRANSFORMADOR DE DISTRIBUCION"]
df.loc[sel.index,"CODELEME"] =  0
sel = df[df['CODIGO'] == 0]
df.loc[sel.index,"CODIGO"] =  df.loc[:,"CODELEME"].astype("int64").abs()
sel = df[df['CODIGO'] == 0]
df.loc[sel.index,"CODIGO"] = str(10) + df.loc[:,"Equipo Ruta Id"].astype(str)
df['CODIGO'] = df['CODIGO'].astype("int64").abs()
    
copiarColumnna("ID_BDI", 1, df)
copiarColumnna("CODELEME", 2, df)
copiarColumnna("Estado", 3, df)
copiarColumnna("PLACA MT COLOCADA", 4, df)
copiarColumnna("ESTADO DEL TRANSFORMADOR", 5, df)
copiarColumnna("VERIFICACIÓN APOYO Y TRANSFORMADOR", 6, df)
copiarColumnna("MOTIVO NO MATRICULACION", 7, df)
copiarColumnna("FOTO MOTIVO NO MATRICULACION", 8, df)
copiarColumnna("Motivo por el cual no se puede matricular", 9, df)
copiarColumnna("Observaciones", 10, df)

#imprimimos el df en un excel
df.to_excel('Informe Tranformadores MLU (GUAJIRA).xlsx', sheet_name='Informe Transformadores MLU', index = False)

#guardar archivo
file_path = filedialog.asksaveasfile(mode='w', defaultextension=".xlsx")
if file_path is None:
  a = 0
else:
    t = time.time()
    with pd.ExcelWriter(file_path.name) as writer:
        df.to_excel(writer, sheet_name='Informe MLU', na_rep='',float_format=None, columns=None, header=True,index=False)
        Workbook = writer.book
        worksheet = writer.sheets['Informe MLU']
        for col_num, value in enumerate(df.columns.values):
            if col_num <= 10 :
                header_format = Workbook.add_format({
                    'bold': True,
                    'text_wrap': False,
                    'valign': 'top',
                    'fg_color': '#77B5FE',
                    'border': 1})
                header_format.set_align('center')
                header_format.set_align('vcenter')
                worksheet.write(0, col_num, value, header_format)
                worksheet.set_column(0,col_num, 20)

elapsed_time = '{0:.2f}'.format(time.time() - t0 - t)
print('Reporte generado en ' + elapsed_time + ' seg')