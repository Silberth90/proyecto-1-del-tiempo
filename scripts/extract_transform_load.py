import requests
import pyodbc
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv() #cargo las variables de entorno
API_KEY = os.getenv("KEY") #llave de la api
conexion = os.getenv("conexion") #url de la base de datos

## funcion para extraer los datos de la api
def extract_weather_data(city):
    url = f'https://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}'
    print(f"extrayendo datos de la API para la ciudad: {city}")
    response = requests.get(url)
    if response.status_code == 200:
        print(f"Datos extraidos correctamente para: {city}")
        data = response.json()
        print(pd.DataFrame([data]))
        return data
    else:
        print(f"Error al extraer datos para la ciudad: {city}")
        return None


## funcion para transformar los datos que toma de la api
def transform_data(data):
    transformarDatos = {
        'ciudad': data['name'],
        'pais':data['sys']['country'],
        'temperatura':data['main']['temp'],
        'clima':data['weather'][0]['description'],
        'humedad':data['main']['humidity'],
        'velocidad_viento':data['wind']['speed'],
        'fecha_hora':datetime.now().strftime('%Y%m%d %H:%M')
        }
    print(pd.DataFrame([transformarDatos]))
    return transformarDatos

## funcion para cargar los datos en la base de datos
def load_data(data, cargarBaseDatos = True):
    if cargarBaseDatos == True:
        conn = pyodbc.connect(conexion)
        cursor = conn.cursor()
        #verifica si los datos ya existen en la base de datos
        cursor.execute("""SELECT * FROM tiempo
                        WHERE ciudad = ? AND pais = ? AND CONVERT(VARCHAR(16), fecha_hora, 120) = ?""",
                        data['ciudad'],
                        data['pais'],
                        data['fecha_hora'])
        existe = cursor.fetchone()
        #si existe, no se cargan los datos
        if existe is not None :
            print("estos datos ya existen en la base de datos")
            return
        else:
            cursor.execute("""INSERT INTO tiempo 
                   (ciudad, pais, temperatura, clima, humedad, velocidad_viento, fecha_hora)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                   data['ciudad'],
                   data['pais'],
                   data['temperatura'],
                   data['clima'],
                   data['humedad'],
                   data['velocidad_viento'],
                   data['fecha_hora'])
            print("LOS DATOS SE CARGARON EN LA BASE DE DATOS")
        conn.commit()
        cursor.close()
        conn.close()
    else:
        print("NO SE CARGARON LOS DATOS EN LA BASE DE DATOS")

## bucle para que el usuario pueda ingresar la ciudad y extraer los datos
while True:
    city = str(input("Ingrese el nombre de la ciudad o 'salir' para terminar: "))

    if city == "salir":
        break

    weather_data = extract_weather_data(city)

    ## si los datos no son nulos, se transforman y se cargan en la base de datos
    if weather_data is not None:
        #transforma los datos
        weather_data_transform = transform_data(weather_data)
        #pregunta si se quiere cargar los datos en la base de datos
        cargarBaseDatos = input("quiere que se cargues los datos en SQL SERVER? (si/no): ").strip().lower() == "si"
        #carga los datos en la base de datos
        load_data(weather_data_transform, cargarBaseDatos)
    else:
        print("no se pudo extraer los datos de la API")


