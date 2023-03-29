import dask
import pandas as pd
import dask.dataframe as dd
import numpy as np
from pandas import ExcelWriter
# from fConsolidarClientes import DistanciaCoord
import tkinter as tk
from tkinter import filedialog
from tkinter import messagebox
from xlwt import Workbook
import xlwt
import time
import sys
from normalize import normalize

from RemStrDuplicated import unique_list
import warnings

# =============================================================================
# Funciones comunes para manejo de informes AP
# =============================================================================
def copiarColumnna(columna,posicion,df):
    copia = str(columna) + "-"
    df.insert(posicion,copia,0)
    df[copia] = df[columna]