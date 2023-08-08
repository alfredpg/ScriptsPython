# -*- coding: utf-8 -*-
"""
Created on Tue Aug  1 14:30:05 2023
@author: Alfredo
"""
# =============================================================================
# LIBRERIAS
# =============================================================================
import pandas as pd
import re

# =============================================================================
# FUNCIONES
# =============================================================================
    # Función para obtener los nombres de las columnas que contienen un número en una fila
def obtener_columnas_con_numero_fila(row, start_column_index, end_column_index):
    columnas_con_numero = []
    for idx, valor in enumerate(row.iloc[start_column_index:end_column_index+1]):
        if str(valor).isdigit():
            columnas_con_numero.append(df.columns[start_column_index + idx])
    return ','.join(columnas_con_numero)

def procesar_columna(df, columna_procesar, columna_guardar):
    def obtener_numeros(cadena):
        numeros = re.findall(r'\b\d+\s*W\b', str(cadena), re.IGNORECASE)
        return [int(numero[:-1]) for numero in numeros]

    def procesar_fila(row):
        valores = obtener_numeros(row[columna_procesar])

        if len(valores) == 1:
            return f"{valores[0]} W"
        else:
            suma_numeros = sum(valores)
            return f"{suma_numeros} W"

    df[columna_guardar] = df.apply(procesar_fila, axis=1)
    return df

# =============================================================================

df_1 = pd.read_excel("BBDD AP.xlsx", header=1)

df = df_1.copy(deep=True) #respaldo


# Especificar el rango de columnas donde se aplicará la función (0 a 3)
start_column_index = 26
end_column_index = 178

df['Tecnología'] = df.apply(obtener_columnas_con_numero_fila, args=(start_column_index, end_column_index), axis=1)

df = procesar_columna(df, columna_procesar='Tecnología', columna_guardar='Potencia')

df.to_csv('BBDD AP_result.csv', sep=';',index=False)
