# LIBRERIAS
from ftplib import FTP
import os
import pandas as pd
import socket

#LISTAS USADAS EN TODO EL PROCESO

#lista para descargar 
# lista_descarga = ['761_20589350','761_20589349','761_20589337']
lista_ruta_servidor = []
lista_descarga = []
lista_id_tx = []
lista_id_ap = []
lista_ruta_temp = []
lista_ruta_doc = []
lista_Error_ap =[]
lista_ok_ap = []
lista_Error_tx =[]
lista_ok_tx = []

# #RECORDATORIOS DOC EXCEL:
# =============================================================================
# TILDAR LA A DE ATLANTICO DEBE QUEDAR ATLÁNTICO
# CAMBIAR ESPACIOS POR _
# CAMBIAR LOS NOMBRES DE LOS TERRITORIOS 01_ 02_ ETC
# =============================================================================
# =============================================================================
#  IndexError: list index out of range = 
#  SIGIFICA QUE NO HAY CARPETA MATRICULACION DENTRO DEL CIRCUITO
#
#  error_perm: 550 CWD failed. "/MLU AIR-E/03_MAGDALENA/PLATO/PLATO__I": directory not found. =
#  ALGO DEBER ESTAR MAL ESCRITO EN EL EXCEL
# =============================================================================
lc1 = "/ImagenesFormsMap/ImagenesCampo/MLU AIR-E/"
lc2 = "/MLU AIR-E/"

#credenciales
usuario = 'lcabrera2'
contraseña = '123456'

if usuario == 'lcabrera2':
    inicio = lc2
else:
    inicio = lc1

df = pd.read_excel('descarga.xlsx') # SE CARGA EL ARCHIVO EXCEL QUE ESTA DENTRO DE LA CARPETA
#rellenar vacias por ceros
df.fillna(0, inplace=True)
#SE CREA RUTA INCIAL A PARTIR DE ARCHIVO EXCEL Y SE EMPAQUETA EN LSITAS
for i in df.index:
    ruta_servidor =inicio+df["TERRITORIO"][i]+"/"+df["SUB_ESTACION"][i]+"/"+df["CIRCUITO"][i]
    lista_ruta_servidor.append(ruta_servidor)
    
    #SE EXTRAEN LOS RUTA ID TX Y SE LISTAN
    n_id_tx =str(int(df["RUTA ID"][i]))
    lista_descarga.append(n_id_tx)
    lista_id_tx.append(n_id_tx)
    
    #SE EXTRAEN LOS RUTA ID AP Y SE LISTAN
    n_id_ap = str(int(df["RUTA ID APOYO"][i]))
    lista_descarga.append(n_id_ap)
    lista_id_ap.append(n_id_ap)
    
    #SE EXTRAEN LOS NOMBRE RUTA DEL EXCEL Y SE LISTAN
    lista_ruta_doc.append(df["RUTA"][i])
    
    # print(i)
# print(lista_ruta_servidor)
print(len(lista_ruta_servidor))

# print(lista_descarga)
# print(len(lista_descarga))

#INICIAMOS SESION EN EL SERVIDOR 
ftp = FTP()
ftp.set_pasv(False)                                     #modo activo
ftp.connect('formap.co', 21, timeout= 5 )              # servidor, puerto y tiempo de espera
ftp.login(usuario, contraseña)                        #credenciales
ftp.encoding = "UTF-8"
print("conexion correcta") 
# print(ftp.getwelcome())                             #mensaje de bienvenida
print(" ")

#SE AÑADE LA CARPETA MATRICULACIO_XXXX EN LA RUTA SERVIDOR
for ruta in lista_ruta_servidor[0:len(lista_ruta_servidor)]:
    while True:
        try:                        
            ftp.cwd(ruta)
            # print(ftp.cwd(ruta))
            carpetas = ftp.nlst()
            # print(carpetas)
            if 'LEVANTAMIENTO' in carpetas:
                # print ('existen archivos temporales')
                carpetas.remove('LEVANTAMIENTO')            #ELIMINO CARPETA LEVANTAMIENTO DE LAS RUTAS POSIBLES
                # print(carpetas)
            # else:
                # print(carpetas)
            # print(ruta)
            ruta_temp = ruta+"/"+carpetas[0]
            # print(ruta_temp)
            # print(" ")
            lista_ruta_temp.append(ruta_temp)
            break
        
        except socket.timeout:# I expect a timeout.  I want other exceptions to crash and give me a trace
            print("Reconectando...")
            ftp.close()
            ftp.set_pasv(False)                                     #modo activo
            ftp.connect('formap.co', 21, timeout= 5)              # servidor, puerto y tiempo de espera
            ftp.login(usuario, contraseña)                        #credenciales
            ftp.encoding = "UTF-8"
            print("conexion correcta")
            
        except Exception as e:                  #SE CAPTRAN LOS ERRORES COMO: NO ENCONTRADO, MAL ESCRITO
            ruta_temp = ruta+"/error"
            lista_ruta_temp.append(ruta_temp)
            # print(ruta_temp)
            #lista_ruta_servidor.remove(ruta)
            break
            
#print(lista_ruta_temp)

#SE AÑADE RUTA EN LA RUTA SERVIDOR Y SE LISTA
n=0
lista_ruta_temp1= []
for ruta in lista_ruta_temp[0:len(lista_ruta_temp)]:
    ruta_temp1 = ruta+"/"+lista_ruta_doc[n]
    n = n+1
    lista_ruta_temp1.append(ruta_temp1)
    
#SE CREAN LAS POSIBLES RUTAS FINALES
n=0
lista_ruta_final_tx = []
lista_ruta_final_ap = []
#RUTA FINAL TX
for ruta in lista_ruta_temp1[0:len(lista_ruta_temp)]:
    ruta_final = ruta+"/"+"749_"+lista_id_tx[n]
    n = n+1
    lista_ruta_final_tx.append(ruta_final)
#RUTA FINAL AP
n=0
for ruta in lista_ruta_temp1[0:len(lista_ruta_temp)]:
    ruta_final = ruta+"/"+"761_"+lista_id_ap[n]
    n = n+1
    lista_ruta_final_ap.append(ruta_final)
# print(lista_ruta_final_tx)
# print(" ")
# print(lista_ruta_final_ap)
print(" ")
print("DE " + str(len(lista_ruta_final_ap)) + " AP BUSCADOS")

#SE VALIDAN LAS RUTASDINALES AP Y SE LISTAN LAS OK Y ERRORES
n=0
for ruta in lista_ruta_final_ap[0:len(lista_ruta_final_ap)]:
    while True:
        id_ap = lista_id_ap[n]
        try:
            ftp.cwd(ruta)
            lista_ok_ap.append(id_ap)
            break
        
        except socket.timeout:# I expect a timeout.  I want other exceptions to crash and give me a trace
            print("Reconectando...")
            ftp.close()
            ftp.set_pasv(False)                                     #modo activo
            ftp.connect('formap.co', 21, timeout= 5)              # servidor, puerto y tiempo de espera
            ftp.login(usuario, contraseña)                       #credenciales
            ftp.encoding = "UTF-8"
            print("conexion correcta")
        
        except Exception as e:
            lista_Error_ap.append(id_ap)
            # print(ruta)
            lista_ruta_final_ap.remove(ruta)
            break
    n = n+1

print("SE ENCONTRARON " + str(len(lista_ruta_final_ap)) + " AP")
# print(len(lista_ruta_final_ap))
print(" ")

#SE VALIDAN LAS RUTASDINALES TX Y SE LISTAN LAS OK Y ERRORES
print("DE " + str(len(lista_ruta_final_tx)) + " TX BUSCADOS")
n=0
for ruta in lista_ruta_final_tx[0:len(lista_ruta_final_tx)]:
    while True:
        id_tx = lista_id_tx[n]
        try:
            ftp.cwd(ruta)
            lista_ok_tx.append(id_tx)
            break
        
        except socket.timeout:# I expect a timeout.  I want other exceptions to crash and give me a trace
            print("Reconectando...")
            ftp.close()
            ftp.set_pasv(False)                                     #modo activo
            ftp.connect('formap.co', 21, timeout= 5)              # servidor, puerto y tiempo de espera
            ftp.login(usuario, contraseña)                        #credenciales
            ftp.encoding = "UTF-8"
            print("conexion correcta")
            
        except Exception as e:
            lista_Error_tx.append(id_tx)
            # print(ruta)
            lista_ruta_final_tx.remove(ruta)
            break
    n = n+1
    
print("SE ENCONTRARON " + str(len(lista_ruta_final_tx)) + " TX")
# print(len(lista_ruta_final_tx))

#AUTORIZACION PARA DESCARGAR
confirma_descarga = "n"
print(" ")
confirma_descarga = input("¿desea comenzar la descargar?  si / no  : ")
print(" ")
#CODIGO DE DESCARGA
if confirma_descarga == "si":    
    n=0
    for nombre_carpeta in lista_ok_tx[0:len(lista_ok_tx)]:  #SE CREAN LAS CARPETAS LOCALES
        try:
            os.mkdir("//10.20.11.240/censo$/RF_Censo/RF_Ises/Adicional/Desconectados/749_"+nombre_carpeta)
        except FileExistsError:
            print("carpeta ya creada: 749_"+str(nombre_carpeta))
    
    for nombre_carpeta in lista_ok_tx[0:len(lista_ok_tx)]:  #SE CARGAN LAS LAS CARPETAS LOCALES      
        while True:
            os.chdir('//10.20.11.240/censo$/RF_Censo/RF_Ises/Adicional/Desconectados/'+"749_"+nombre_carpeta)
            print("749_"+nombre_carpeta+" "+str(n))
            try:
                ftp.cwd(lista_ruta_final_tx[n])         #SE BUSCA LA RUTA EN EL SERVIDOR
                # ftp.dir() 
                archivos = ftp.nlst()
                # print(archivos)
                if 'Temp' in archivos:              #SE ELIMINA LA CARPETA TEMP DE LO QUE SE DESCARGARA
                    # print ('existen archivos temporales')
                    archivos.remove('Temp')
                    # print(archivos)
                    
                else:
                    print ('NO existen archivos temporales')
                    #print(archivos)
                    
                for archivo in archivos[0:len(archivos)]:   #SE PROCEDE A DESCARGAR
                    abrir = open(archivo, 'wb')
                    ftp.retrbinary("RETR "+ archivo, abrir.write)
                    print("descargando")
                break
            except socket.timeout:# I expect a timeout.  I want other exceptions to crash and give me a trace
                print("Reconectando...")
                ftp.close()
                ftp.set_pasv(False)                                     #modo activo
                ftp.connect('formap.co', 21, timeout= 5)              # servidor, puerto y tiempo de espera
                ftp.login(usuario, contraseña)                        #credenciales
                ftp.encoding = "UTF-8"
                print("conexion correcta")
        n = n+1            
    print("------- termenino descarga tx-------")
    
    os.chdir('//10.20.11.240/censo$/RF_Censo/RF_Ises/Adicional/Desconectados')
    
    n=0
    for nombre_carpeta in lista_ok_ap[0:len(lista_ok_ap)]: #SE CREAN LAS CARPETAS LOCALES
        try:
            os.mkdir("//10.20.11.240/censo$/RF_Censo/RF_Ises/Adicional/Desconectados/761_"+nombre_carpeta)
        except FileExistsError:
            print("apoyo duplicado / carpeta ya creada: 761_"+str(nombre_carpeta))
        
    for nombre_carpeta in lista_ok_ap[0:len(lista_ok_ap)]: #SE CARGAN LAS LAS CARPETAS LOCALES
        while True:    
            os.chdir('//10.20.11.240/censo$/RF_Censo/RF_Ises/Adicional/Desconectados/'+"761_"+nombre_carpeta)
            print("761_"+nombre_carpeta+" "+str(n))
            try:
                ftp.cwd(lista_ruta_final_ap[n])              #SE BUSCA LA RUTA EN EL SERVIDOR
                # ftp.dir() 
                archivos = ftp.nlst()
                # print(archivos)
                if 'Temp' in archivos:              #SE ELIMINA LA CARPETA TEMP DE LO QUE SE DESCARGARA
                    # print ('existen archivos temporales')
                    archivos.remove('Temp')
                    # print(archivos)
                    
                else:
                    print ('NO existen archivos temporales')
                    #print(archivos)
                    
                for archivo in archivos[0:len(archivos)]:   #SE PROCEDE A DESCARGAR
                    abrir = open(archivo, 'wb')
                    ftp.retrbinary("RETR "+ archivo, abrir.write)
                    print("descargando")
                break
            except socket.timeout:# I expect a timeout.  I want other exceptions to crash and give me a trace
                print("Reconectando...")
                ftp.close()
                ftp.set_pasv(False)                                     #modo activo
                ftp.connect('formap.co', 21, timeout= 5)              # servidor, puerto y tiempo de espera
                ftp.login(usuario, contraseña)                        #credenciales
                ftp.encoding = "UTF-8"
                print("conexion correcta")
        n = n+1
            
    print("------- termenino descarga ap-------")

else:
    print("no se descargo nada")    #EN CASO DE NO COLOCAR si 

ftp.close()

