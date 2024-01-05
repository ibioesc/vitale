from sqlalchemy import create_engine, MetaData, Table, select ,update,text
from sqlalchemy.orm import Session
import psycopg2
import pandas as pd
import json
import csv
import os
import datetime

# Conexión a la base de datos PostgreSQL
# Reemplaza 'postgresql://usuario:contraseña@localhost:5432/nombre_de_base_de_datos' con tu propia cadena de conexión

conexion_parametros = {
    'host': 'localhost',
    'port': '5432',
    'database': 'db_vitale',
    'user': 'postgres',
    'password': 'mysecretpassword'
}
engine = create_engine(F'postgresql+psycopg2://{conexion_parametros["user"]}:{conexion_parametros["password"]}@{conexion_parametros["host"]}:{conexion_parametros["port"]}/{conexion_parametros["database"]}')
con = engine.connect()
conexion = psycopg2.connect(**conexion_parametros)


# Definición de la tabla
metadata = MetaData()

    
def pr():
    try:
        metadata = MetaData()

        tbl_pacientes = Table('tbl_pacientes', metadata, autoload_with=engine)
        tbl_citas = Table('tbl_citas', metadata, autoload_with=engine)

        # Campos específicos a seleccionar (en este caso, seleccionamos todos los campos con '*')

        # Establecer la conexión y obtener un objeto de conexión
        resul = []
        resul1 = {}
        with engine.connect() as connection:
            # Construir y ejecutar la consulta SQLAlchemy
            consulta = select(tbl_pacientes, tbl_citas).select_from(tbl_pacientes.join(tbl_citas, tbl_pacientes.c.ID == tbl_citas.c.ID))

            resultados = connection.execute(consulta)
            nombres_columnas = resultados.keys()

            # Imprimir los nombres de las columnas
            # print("Nombres de las columnas:")
            # for nombre_columna in nombres_columnas:
            #     print(nombre_columna)

# Convertir los resultados a un objeto JSON
            resultados_json = []
            

            for resultado in resultados:
                # Crear un diccionario para cada fila
                fila_dict = {nombre_columna: valor for nombre_columna, valor in zip(nombres_columnas, resultado)}
                resultados_json.append(fila_dict)

            # # Convertir la lista de diccionarios a una cadena JSON
            # resultados_json_str = json.dumps(resultados_json, indent=2)

            # # Imprimir o guardar la cadena JSON
            # print(resultados_json_str)
            # RMKeyView(['ID', 'identificacion', 'nombre', 'tipo_identificacion', 'direccion', 'direccion_original', 'latitud', 'longitud', 'barrio', 'municipio', 'celular', 'telefono', 'telefono2', 'plan_salud', 'zona', 'tratamiento', 'medicamento', 'genero', 'fecha_nace', 'observaciones', 'piso', 'depto', 'ID_1', 'fecha', 'hora', 'id_paciente', 'id_profesional', 'id_auxiliar', 'duracion', 'estado', 'id_cita_cliente', 'observacion'])


        # Imprimir los resultados
        
        # for resultado in resultados:
        #    paciente = {'ewaulr':resultado}
                        # barrio
                        # municipio
                        # celular
                        # telefono
                        # telefono2
                        # plan_salud
                        # zona
                        # tratamiento
                        # medicamento
                        # genero
                        # fecha_nace
                        # observaciones
                        # piso
                        # depto
                        # ID_1
                        # fecha
                        # hora
                        # id_paciente
                        # id_profesional
                        # id_auxiliar
                        # duracion
                        # estado
                        # id_cita_cliente

    except (Exception , TypeError) as err:
        print('004: Error de consulta: '+str(err))
def consulta_registro(identificacion = None, boleano_consulta =None):
    try:
        list_resul = []
        metadata = MetaData()
        nombre_tabla =  Table('tbl_pacientes', metadata, autoload_with=engine)
        campo_deseado = nombre_tabla.c.identificacion  # Reemplaza 'edad' con el nombre real de tu campo
        condicion_where = campo_deseado == identificacion
        with engine.connect() as connection:
            # Realizar una consulta para seleccionar solo el campo deseado
            consulta = nombre_tabla.select().where(condicion_where)
            consulta_persona = nombre_tabla.select()

            # Ejecutar la consulta y recuperar los resultados
            resultados = connection.execute(consulta).fetchall()
            resultados_persona = connection.execute(consulta_persona).fetchall()
            for  resul in resultados_persona:
                list_resul.append({
                    'Direccion': resul[4],
                    'Departamento': resul[9],
                    'Localidad': resul[9],
                    'Nombre empresa o lugar': resul[2],
                    '# de contrato': resul[1],
                    'telefono': resul[11],
                    'email': '',
                    'emailCc': '',
                    'latitud': '',
                    'longitud': '',
                    'idCliente': '',
                    'atributos dinamicos': '',
                    'Barrio': resul[8],
                    'Referencia (opcional)': resul[8],
                    'Tipo de identificación del paciente': resul[3],
                    'Telefo#2 Paciente': resul[11],
                    'Telefo#3 Paciente': '',
                    'Edad': '',
                    'Fecha nacimiento paciente': resul[18],
                    'Sexo del paciente': resul[17],
                    'Plan de salud': resul[13],
                    'Tratamiento': resul[15],
                    'Zona Geografica': resul[14],
                    'Duración de la cita': '30',
                    'Medico': '',
                    'Telefono del Medico': '',
                })
                
            columns = ['Direccion',
                       'Departamento',
                       'Localidad',
                       'Nombre empresa o lugar',
                       '# de contrato','telefono',
                       'email','emailCc','latitud',
                       'longitud','idCliente',
                       'atributos dinamicos',
                       'Barrio','Referencia (opcional)',
                       'Tipo de identificación del paciente',
                       'Telefo#2 Paciente',
                       'Telefo#3 Paciente',
                       'Edad',
                       'Fecha nacimiento paciente',
                       'Sexo del paciente',
                       'Plan de salud',
                       'Tratamiento',
                       'Zona Geografica',
                       'Duración de la cita',
                       'Medico',
                       'Telefono del Medico'
                       ]
            nombre_carpeta =  datetime.datetime.now()
            nombre_carpeta = nombre_carpeta.strftime("%d-%m-%Y,%H,%M,%S")
            nombre_carpeta = './documentCSV/'+str(nombre_carpeta)
            os.mkdir(str(nombre_carpeta))

            with open(f'{nombre_carpeta}/puntos.csv', mode='w',encoding="utf-8") as file:
                writer = csv.DictWriter(file, delimiter=',', fieldnames=columns )
                writer.writeheader()
                for course in list_resul:
                    writer.writerow(course)
            if   len(resultados) >=1  and boleano_consulta != True:
                return True, None
            elif boleano_consulta:
                for resultado in resultados:
                    id_usuario = resultado[0]
                return False, id_usuario
            else:
             return False, None
    except (Exception , TypeError) as err:
        print('004: Error de consulta: '+str(err))
        
        
def update_cita(id_cita :int , estado_cita :str):
    try:

#         # tbl_citas = Table('tbl_citas', metadata, autoload_with=engine)
#         tbl_citas = Table('tbl_pacientes',metadata, autoload_with=engine)
#         campo_deseado = tbl_citas.c.ID  # Reemplaza 'edad' con el nombre real de tu campo
#         condicion_where = campo_deseado == "23"
# # Crea la conexión a la base de datos
#         with engine.connect() as connection:
#             # stmt = update(tbl_citas).where(tbl_citas.c.identificacion == '1038816379').values(barrio ='Nuevo Valor')

# # Ejecuta la sentencia de actualización
#             # print(stmt)
#             # stmt = text(f'''UPDATE tbl_pacientes SET nombre = 'R' WHERE "ID"  = 23''') 
#             stmt = text('''UPDATE tbl_pacientes SET nombre = 'RIMO' WHERE identificacion = "1038816379"''') 
            

#             # Ejecuta la sentencia de actualización
#             connection.execute(stmt)
        cur = conexion.cursor()
        # Ejecutamos una consulta
        cur.execute( f'''UPDATE tbl_citas SET estado = '{estado_cita}' WHERE "ID"  = {id_cita};''' )
        updated_rows = cur.rowcount
        conexion.commit()
        cur.close()
        conexion.close()
        resultados_dic = {
                "status": 200,
                "error": None,
                "message": "",
                "result":'Actualizado correctamente'
                    }  
        return resultados_dic

    except (Exception , TypeError) as err:
        print('004: Error de actualizar: '+str(err))
        resultados_dic = {
                "status": 404,
                "error": str(err),
                "message": "Error de consulta",
                "result":''
                    } 
        return resultados_dic 
        

if __name__ == "__main__":
    consulta_registro()
    # update_cita(1,"I")
    # pr()