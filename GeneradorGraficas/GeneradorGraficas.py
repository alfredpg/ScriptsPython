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

import warnings
import datetime
from datetime import date, time, datetime, timedelta
import matplotlib.pyplot as plt

df = pd.read_excel('AcerosTuria - Final - Graficas.xlsb', engine='pyxlsb')

x = df['Fecha'].tolist()
x = pd.unique(x)

for i in x:
    dfTemp = df[df['Fecha'] == i]
    # dfTemp.to_excel('analisis.xlsx', sheet_name="otra", index = False)
    # print(i)

df.plot()
