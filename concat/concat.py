# =============================================================================
# Codigo verificacion de rutas de entidades lev 
# =============================================================================

import pandas as pd
import time
from tkinter import filedialog
import sys

# cargamos informe como dataframe
file_path = filedialog.askopenfilename(defaultextension=".xls")
if file_path is None:
    # quit()
    sys.exit()

t0 = time.time()

df = pd.read_excel(file_path, sheet_name="Lev_E_Adic", header=2)
df = df.drop(columns = 'Unnamed: 0')

df_buscar = pd.read_excel(file_path, sheet_name="Ruta_E_Adic")
df_buscar = df_buscar.drop(columns = 'Unnamed: 0')

print("la cantidad de Rutas a verificar es de: " + str(len(df)))

# tiempo de cargue
a = time.time() - t0
tiempoCargue = '{0:.2f}'.format(a)
print("El tiempo de cargue es: " + str(tiempoCargue) + " seg")

#rellenar vacias por ceros
df['CODIGO'].fillna(0, inplace=True)
df['CODIGO'] = df['CODIGO'].astype("int64").abs() #CONVERTIMOS A ENTERO ABSOLUTO

df['RUTA'] = df.loc[:,'RUTA'].apply(str.replace,args=('ATLÃNTICO', 'ATLÁNTICO')) #remplazamos
df['RUTA'] = df.loc[:,'RUTA'].apply(str.replace,args=('MATRICULACIÃ“N', 'MATRICULACIÓN')) #remplazamos
df['RUTA'] = df.loc[:,'RUTA'].apply(str.replace,args=('SUPERVISIÃ“N_MAT', 'SUPERVISIÓN_MAT')) #remplazamos
df['RUTA'] = df.loc[:,'RUTA'].apply(str.replace,args=('SUP_EJECUCIÃ“N', 'SUP_EJECUCIÓN')) #remplazamos

df = df.sort_values("CODIGO", ascending=True)
df = df.reset_index()
df = df.drop(columns = 'index')

freq = df['CODIGO'].value_counts()
freq = freq.reset_index()
freq = freq.sort_values("index", ascending=True)
freq = freq.reset_index()
freq = freq.drop(columns = 'level_0')

n = 0
nFila = 0
datoCodigoAnt = 0

for nFila in df.index:
    datoCodigo = df.loc[nFila,"CODIGO"]
    if datoCodigo == datoCodigoAnt :
        n = n+1
        df.loc[nFila,"CODIGO_CONSECUTIVO"] = df.loc[nFila,"CODIGO"].astype(str)+"_0"+str(n)
    else:
        n=1
        df.loc[nFila,"CODIGO_CONSECUTIVO"] = df.loc[nFila,"CODIGO"].astype(str)+"_0"+str(n)
    datoCodigoAnt = datoCodigo

# tiempo de secuencia
b = time.time() - a - t0
tiempoSecuencia = '{0:.2f}'.format(b)
print("El tiempo para generar la secuencia es: "+ str(tiempoSecuencia)+ " seg")

#df_buscar1 = pd.concat([df_buscar["RUTA_BUSCAR"],df_buscar["RUTA_BUSCAR_1"]],axis=0)
df_buscar1 = df_buscar["RUTA_BUSCAR"]
df_buscar1.dropna(inplace=True)
#print(len(df_buscar["RUTA_BUSCAR"]))

Sel = df[df['TEMPORAL'] == "No"].index
df.loc[Sel,"Ruta Existe [Si/No]"] =  df["RUTA"].isin(df_buscar1).astype(str)
df["Ruta Existe [Si/No]"].replace('False', 'No', inplace=True)
df["Ruta Existe [Si/No]"].replace('True', 'Si', inplace=True)
df["Ruta Existe [Si/No]"].fillna('N/A', inplace=True)

df["CODIGO_CONSECUTIVO_FINAL"] = df["CODIGO_CONSECUTIVO"]+df["Ruta Existe [Si/No]"]

df.to_excel('VALIDACION.xlsx', sheet_name='E', index = False)

tiempoTotal = '{0:.2f}'.format(time.time() - t0)
print("El tiempo total de ejecucion es: " + str(tiempoTotal)+ " seg")
