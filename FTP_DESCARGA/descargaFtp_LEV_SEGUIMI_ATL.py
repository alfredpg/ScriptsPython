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
lista_Error_rutas_servidor =[]
lista_descargado =[] 
lista_id_carpetas_existentes = []

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
lc2 = "/MLU AIR-E/01_ATLÁNTICO_NORTE/SEGUIMIENTOS_OPERATIVOS_ATL/SUPERVISIÓN_ATL/VERIFICACIÓN_OC_ATL/"

#credenciales
usuario = 'lcabrera2'
contraseña = '123456'


if usuario == 'lcabrera2':
    inicio = lc2
else:
    inicio = lc1

df = pd.read_excel('descarga_lev_apoyo.xlsx') # SE CARGA EL ARCHIVO EXCEL QUE ESTA DENTRO DE LA CARPETA
#rellenar vacias por ceros
df.fillna(0, inplace=True)
#SE CREA RUTA INCIAL A PARTIR DE ARCHIVO EXCEL Y SE EMPAQUETA EN LSITAS
for i in df.index:
    ruta_servidor =inicio+str(df["RUTA"][i])
    lista_ruta_servidor.append(ruta_servidor)
   
    #SE EXTRAEN LOS RUTA ID AP Y SE LISTAN
    n_id_ap = str(df["RUTA ID"][i])
    lista_descarga.append(n_id_ap)
    lista_id_ap.append(n_id_ap)
    
    # print(i)
    
# print(lista_ruta_servidor)
print(len(lista_ruta_servidor))

# print(lista_descarga)
# print(len(lista_descarga))

#INICIAMOS SESION EN EL SERVIDOR 
ftp = FTP()
ftp.set_pasv(False)                                     #modo activo
ftp.connect('formap.co', 21, timeout= 10)              # servidor, puerto y tiempo de espera
ftp.login(usuario, contraseña)                        #credenciales
ftp.encoding = "UTF-8"
print("conexion correcta") 
# print(ftp.getwelcome())                             #mensaje de bienvenida
print(" ")
    
#SE CREAN LAS POSIBLES RUTAS FINALES
n=0
lista_ruta_final_tx = []
lista_ruta_final_ap = []
    
#RUTA FINAL AP
n=0
for ruta in lista_ruta_servidor[0:len(lista_ruta_servidor)]:
    ruta_final = ruta+"/"+"762_"+lista_id_ap[n]
    n = n+1
    lista_ruta_final_ap.append(ruta_final)
    
# print(lista_ruta_final_tx)
# print(" ")
# print(lista_ruta_final_ap)

print(" ")
print("DE " + str(len(lista_ruta_final_ap)) + " Avisos BUSCADOS")

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
            ftp.connect('formap.co', 21, timeout= 10)              # servidor, puerto y tiempo de espera
            ftp.login(usuario, contraseña)                       #credenciales
            ftp.encoding = "UTF-8"
            print("conexion correcta")
        
        except Exception as e:
            lista_Error_ap.append(id_ap)
            lista_Error_rutas_servidor.append(ruta)
            # print(ruta)
            lista_ruta_final_ap.remove(ruta)
            break
    #print(n)
    n = n+1

print("SE ENCONTRARON " + str(len(lista_ruta_final_ap)) + " Avisos")
# print(len(lista_ruta_final_ap))
print(" ")

#AUTORIZACION PARA DESCARGAR
confirma_descarga = "si"
print(" ")
confirma_descarga = input("¿desea comenzar la descargar?  si / no  : ")
print(" ")
#CODIGO DE DESCARGA
if confirma_descarga == "si":    
    n=0
    for nombre_carpeta in lista_ok_ap[0:len(lista_ok_ap)]: #SE CREAN LAS CARPETAS LOCALES
        try:
            os.mkdir('//10.20.11.240/censo$/RF_Censo/RF_Ises/Adicional/Antena/'+"794_"+nombre_carpeta)
        except FileExistsError:
            #print("apoyo duplicado / carpeta ya creada: 762_"+str(nombre_carpeta))
            lista_id_carpetas_existentes.append(nombre_carpeta)
    
    print("VALIDAREMOS LA CONEXION PARA COMENZAR LA DESCARGA")
    ftp.close()
    ftp.set_pasv(False)                                     #modo activo
    ftp.connect('formap.co', 21, timeout= 10)              # servidor, puerto y tiempo de espera
    ftp.login(usuario, contraseña)                       #credenciales
    ftp.encoding = "UTF-8"
    print("conexion correcta")
    print(" ")      
    
    for nombre_carpeta in lista_ok_ap[0:len(lista_ok_ap)]: #SE CARGAN LAS LAS CARPETAS LOCALES
        while True:
            os.chdir('//10.20.11.240/censo$/RF_Censo/RF_Ises/Adicional/Antena/'+"794_"+nombre_carpeta)
            print(" ")
            print("794_"+nombre_carpeta+" "+str(n+1))
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
                    while True:
                        try:
                            abrir = open(archivo, 'wb')
                            ftp.retrbinary("RETR "+ archivo, abrir.write)
                            print("descargando")
                            break
                            
                        except:# I expect a timeout.  I want other exceptions to crash and give me a trace
                            print("Reconectando para seguir la descarga...")
                            ftp.close()
                            ftp.set_pasv(False)                                     #modo activo
                            ftp.connect('formap.co', 21, timeout= 10)              # servidor, puerto y tiempo de espera
                            ftp.login(usuario, contraseña)                        #credenciales
                            ftp.encoding = "UTF-8"
                            ftp.cwd(lista_ruta_final_ap[n])
                                            
                break
            except socket.timeout:# I expect a timeout.  I want other exceptions to crash and give me a trace
                print("Reconectando...")
                ftp.close()
                ftp.set_pasv(False)                                     #modo activo
                ftp.connect('formap.co', 21, timeout= 10)              # servidor, puerto y tiempo de espera
                ftp.login(usuario, contraseña)                        #credenciales
                ftp.encoding = "UTF-8"

        n = n+1
        lista_descargado.append(nombre_carpeta)
    print("------- termenino descarga avisos-------")

else:
    print("no se descargo nada")    #EN CASO DE NO COLOCAR si 

ftp.close()

#df_descargado = pd.DataFrame(lista_descargado)
#df_descargado.to_excel('C:/Users/P568/Desktop/PROYECTOS_ISES/FTP_DESCARGA/descargados.xlsx', sheet_name='Descargados', index = False)
