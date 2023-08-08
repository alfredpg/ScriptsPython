# #RECORDATORIOS DOC EXCEL:
# =============================================================================
# TILDAR LA A DE ATLANTICO DEBE QUEDAR ATLÁNTICO
# CAMBIAR ESPACIOS POR _
# CAMBIAR LOS NOMBRES DE LOS TERRITORIOS 01_ 02_ ETC
# =============================================================================
# LIBRERIAS
# =============================================================================
from ftplib import FTP
import os
import pandas as pd
import socket
import time
import sys
# =============================================================================
# FUNCIONES
# =============================================================================

def reconectar_ftp(mensajeInicio):
    global usuario, contraseña
    while True:
        try:
            print(mensajeInicio)
            ftp = FTP()
            ftp.close()
            ftp.set_pasv(False)  # Modo activo
            ftp.connect('formap.co', 21, timeout=10)  # Servidor, puerto y tiempo de espera
            ftp.login(usuario, contraseña)  # Credenciales
            ftp.encoding = "UTF-8"
            print("conexion ftp correcta")
            return ftp
        except Exception as e:
            print(f"Error al conectar FTP: {e}")
            print("Reintentando en 5 segundos...")
            time.sleep(5)

def formatear_ruta(cadena):
    return cadena.replace("\\", "/")

def obtener_ruta_script():
    # Obtener la ruta del archivo .py que está siendo ejecutado
    ruta_script = os.path.abspath(sys.argv[0])
    return os.path.dirname(ruta_script)

#LISTAS USADAS EN TODO EL PROCESO
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
lista_id_archivos_sin_permiso =[]
# =============================================================================
# VARIABLES CAMBIANTES
# =============================================================================
#Ruta donde se descargaran
rutaDescarga = r"//10.20.11.240/censo$/RF_Censo/Alfredo/prueba01082023"

#prefijo del tipo equipo
prefijo = "762_"

#credenciales
usuario = 'lcabrera2'
contraseña = '123456'
#nombre del excel a descargar
nombreExcel = "A1.xlsx"
# =============================================================================
excepcion_previa = ""
rutaScript = obtener_ruta_script()
rutaDescargaFormat = formatear_ruta(rutaDescarga)

lc1 = "/ImagenesFormsMap/ImagenesCampo/MLU AIR-E/"
lc2 = "/MLU AIR-E/"

if usuario == 'lcabrera2':
    inicio = lc2
else:
    inicio = lc1

df = pd.read_excel(nombreExcel) # SE CARGA EL ARCHIVO EXCEL QUE ESTA DENTRO DE LA CARPETA
nombreDescargados = "descargados_"+nombreExcel
#rellenar vacias por ceros
df.fillna(0, inplace=True)
#SE CREA RUTA INCIAL A PARTIR DE ARCHIVO EXCEL Y SE EMPAQUETA EN LISTAS
for i in df.index:
    ruta_servidor =inicio+str(df["TERRITORIO"][i])+"/"+str(df["SUB_ESTACION"][i])+"/"+str(df["CIRCUITO"][i])+"/LEVANTAMIENTO/"+str(df["RUTA"][i])
    lista_ruta_servidor.append(ruta_servidor)
   
    #SE EXTRAEN LOS RUTA ID AP Y SE LISTAN
    n_id_ap = str(df["RUTA ID"][i])
    lista_descarga.append(n_id_ap)
    lista_id_ap.append(n_id_ap)
    
    #print(i)
    
# print(lista_ruta_servidor)
print(len(lista_ruta_servidor))

# print(lista_descarga)
# print(len(lista_descarga))

#INICIAMOS SESION EN EL SERVIDOR 
ftp = reconectar_ftp("") 
# print(ftp.getwelcome())                             #mensaje de bienvenida
print(" ")
    
#SE CREAN LAS POSIBLES RUTAS FINALES
n=0
lista_ruta_final_tx = []
lista_ruta_final_ap = []
    
#RUTA FINAL AP
n=0
for ruta in lista_ruta_servidor[0:len(lista_ruta_servidor)]:
    ruta_final = ruta+"/"+prefijo+lista_id_ap[n]
    n = n+1
    lista_ruta_final_ap.append(ruta_final)
# print(" ")
# print(lista_ruta_final_ap)

print(" ")
print("DE " + str(len(lista_ruta_final_ap)) + " AP BUSCADOS")

#SE VALIDAN LAS RUTASDINALES AP Y SE LISTAN LAS OK Y ERRORES
n=0
for ruta in lista_ruta_final_ap[0:len(lista_ruta_final_ap)]:
    while True:
        if n != 0 and n % 3000 == 0:
            ftp = reconectar_ftp(f"Reconectando de la pausa {n}")
            
        id_ap = lista_id_ap[n]
        try:
            ftp.cwd(ruta)
            lista_ok_ap.append(id_ap)
            break
        
        except socket.timeout:# I expect a timeout.  I want other exceptions to crash and give me a trace
            ftp = reconectar_ftp("Reconectando...")
        
        except Exception as e:
            print(f"Ocurrió un error: {e}")
            lista_Error_ap.append(id_ap)
            lista_Error_rutas_servidor.append(ruta)
            # print(ruta)
            lista_ruta_final_ap.remove(ruta)
            break
    print(str(n+1)+" analizados de "+ str(len(lista_ruta_servidor)))
    n = n+1

print("SE ENCONTRARON " + str(len(lista_ruta_final_ap)) + " AP")
# print(len(lista_ruta_final_ap))
print(" ")

#AUTORIZACION PARA DESCARGAR
confirma_descarga = "SI"
print(" ")
confirma_descarga = input("¿desea comenzar la descargar?  si / no  : ")
print(" ")
#CODIGO DE DESCARGA
if confirma_descarga.lower() == "si":
    print("PREPARANDOSE PARA LA DESCARGA")     
    
    for nombre_carpeta in lista_ok_ap[0:len(lista_ok_ap)]: #SE CREAN LAS CARPETAS LOCALES
        while True:
            try:
                os.mkdir(rutaDescargaFormat+'/'+prefijo+nombre_carpeta)
                break
            except FileExistsError:
                #print("apoyo duplicado / carpeta ya creada: 762_"+str(nombre_carpeta))
                lista_id_carpetas_existentes.append(nombre_carpeta)
                break
            except Exception as e:
                if str(type(e)) != excepcion_previa:
                    print(f"Ocurrió un error: {e}")
                excepcion_previa = str(type(e))
    
    #print("VALIDAREMOS LA CONEXION PARA COMENZAR LA DESCARGA")
    ftp = reconectar_ftp("VALIDANDO LA CONEXION PARA COMENZAR LA DESCARGA")
    print(" ")      
    
    n=0
    for nombre_carpeta in lista_ok_ap[0:len(lista_ok_ap)]: #SE CARGAN LAS LAS CARPETAS LOCALES
        while True:
            os.chdir(rutaDescargaFormat+'/'+prefijo+nombre_carpeta)
            print(" ")
            print(prefijo+nombre_carpeta+" "+str(n+1)+'/'+str(len(lista_ruta_final_ap)))
            try:
                ftp.cwd(lista_ruta_final_ap[n])              #SE BUSCA LA RUTA EN EL SERVIDOR
                # ftp.dir() 
                archivos = ftp.nlst()
                # print(archivos)
                if 'Temp' in archivos:              #SE ELIMINA LA CARPETA TEMP DE LO QUE SE DESCARGARA
                    # print ('existen archivos temporales')
                    archivos.remove('Temp')
                    print ('se ELIMINO archivos temporales')                    
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
                        
                        except PermissionError:
                            print("No tienes permisos.")
                            lista_id_archivos_sin_permiso.append(nombre_carpeta)
                            break
                        
                        except Exception as e:
                            print(f"Ocurrió un error: {e}")# I expect a timeout.  I want other exceptions to crash and give me a trace
                            ftp = reconectar_ftp("Reconectando para seguir la descarga...") 
                            ftp.cwd(lista_ruta_final_ap[n])
                        
                break
            except socket.timeout:# I expect a timeout.  I want other exceptions to crash and give me a trace
                ftp = reconectar_ftp("Reconectando ftp...") 
            except Exception as e:
                print(f"Ocurrió un error: {e}")# I expect a timeout.  I want other exceptions to crash and give me a trace
                ftp = reconectar_ftp("Reconectando para seguir la descarga...")
                
            
        n = n+1
        lista_descargado.append(nombre_carpeta)
    print("------- termenino descarga ap-------")

    lista_id_archivos_sin_permiso = list(set(lista_id_archivos_sin_permiso))
    df_descargado = pd.DataFrame(lista_descargado)
    os.chdir(rutaScript)
    df_descargado.to_excel(nombreDescargados, sheet_name='Descargados', index = False)
    print(nombreDescargados)
else:
    print("no se descargo nada")    #EN CASO DE NO COLOCAR si 

ftp.close()

