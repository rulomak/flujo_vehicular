import pandas as pd
import pyodbc

# lectura y transformacion de datos

df = pd.read_csv('dataset/dataset_flujo_vehicular.csv')

## separa la columna Hora en fecha y hora  
df['Fecha'] = df['HORA'].apply(lambda x: x.split(':')[0])
df['Hora_s'] = df['HORA'].apply(lambda x: x.split(':')[1:4])
df.drop(columns=['HORA'], inplace=True)
df["Hora"] = df['Hora_s'].apply(lambda x: f"{x[0]:02}:{x[1]:02}:{x[2]:02}")
df.drop(columns=['Hora_s'], inplace=True)

##  Convierte la columna "fecha" y "Hora" a su tipo  
df['Fecha'] = pd.to_datetime(df['Fecha']).dt.date
df['Hora'] = pd.to_datetime(df['Hora'], format='%H:%M:%S').dt.time

print("Transformacion lista")



# conexion con SQL server 
###  conexión a SQL Server ( datos de prueba )
server = '.'
database = 'flujo_vehicular'
username = 'sa'
password = '1234' 

# Crea una cadena de conexión
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
print("conexion establecida con Sql server")


try:
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # Itera sobre las filas del DataFrame e inserta los valores en la tabla SQL Server
    for _, fila in df.iterrows():
        # Verifica y maneja los valores nulos
        latitud = fila['LATITUD'] if pd.notnull(fila['LATITUD']) else None
        longitud = fila['LONGITUD'] if pd.notnull(fila['LONGITUD']) else None

        # parámetros en la consulta 
        query = "INSERT INTO BsAs_city (CODIGO_LOCACION, CANTIDAD, SENTIDO, LATITUD, LONGITUD, Fecha, Hora) VALUES (?, ?, ?, ?, ?, ?, ?)"
        valores = (fila['CODIGO_LOCACION'], fila['CANTIDAD'], fila['SENTIDO'], latitud, longitud, fila['Fecha'], fila['Hora'])
        
        cursor.execute(query, valores)

    # Confirma la inserción
    conn.commit()
    print("Datos insertados correctamente.")

    # Cierra la conexión
    conn.close()

except pyodbc.Error as e:
    print(f"Ocurrió un error: {str(e)}")

                



