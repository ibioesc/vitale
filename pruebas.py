from sqlalchemy import create_engine, MetaData, Table, select ,update,text,insert, exc
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
        
def consulta_registro(identificacion = None, boleano_consulta = None, usuario = None):
    try:
        metadata = MetaData()
        # nombre_tabla =  Table('tbl_pacientes', metadata, autoload_with=motor_sqlalchemy)
        nombre_tabla =  Table('tbl_pacientes', metadata, autoload_with=engine, schema='sch_vitale')
        tabla_usuarios =  Table('tbl_usuarios', metadata, autoload_with=engine, schema='sch_vitale')
        campo_deseado = nombre_tabla.c.identificacion  # Reemplaza 'edad' con el nombre real de tu campo
        campo_email = tabla_usuarios.c.email  # Reemplaza 'edad' con el nombre real de tu campo
        condicion_where = campo_deseado == identificacion
        condicion_usuarios = campo_email == usuario
        with engine.connect() as connection:
            # Realizar una consulta para seleccionar solo el campo deseado
            consulta = nombre_tabla.select().where(condicion_where)
            consulta_persona = nombre_tabla.select()
            consulta_usuarios = tabla_usuarios.select().where(condicion_usuarios)

            # Ejecutar la consulta y recuperar los resultados
            resultados = connection.execute(consulta).fetchall()
            resultados_persona = connection.execute(consulta_persona).fetchall()
            resultados_usuarios = connection.execute(consulta_usuarios).fetchall()



            
            if   len(resultados) >=1  and boleano_consulta != True:
                return True, None,
            elif boleano_consulta:
                for resultado in resultados:
                    id_usuario = resultado[0]
                return False, id_usuario
            elif resultados_usuarios:
                resultados_usuarios = True

            else:
             return False, None,resultados_persona,resultados_usuarios
    except Exception as err:
        print('004: Error de consulta: '+str(err))
        # logging.error(f"Error de consulta registro: {str(err)}")
        # raise HTTPException(status_code=400,
        #                     detail={'Error':True,
        #                             'Message':'Error de consulta registro',
        #                             'Result':''})
   
   
from sqlalchemy import MetaData, Table
from sqlalchemy.sql import and_

def consulta_registro(identificacion=None, boleano_consulta=None, usuario=None):
    try:
        metadata = MetaData()
        nombre_tabla = Table('tbl_pacientes', metadata, autoload_with=engine, schema='sch_vitale')
        tabla_usuarios = Table('tbl_usuarios', metadata, autoload_with=engine, schema='sch_vitale')

        campo_deseado = nombre_tabla.c.identificacion
        campo_email = tabla_usuarios.c.email

        condicion_where = campo_deseado == identificacion
        condicion_usuarios = campo_email == usuario

        with engine.connect() as connection:
            consulta_persona = nombre_tabla.select().where(condicion_where)
            consulta_usuarios = tabla_usuarios.select().where(condicion_usuarios)

            resultados_persona = connection.execute(consulta_persona).fetchall()
            resultados_usuarios = connection.execute(consulta_usuarios).fetchall()

            if len(resultados_persona) >= 1 and not boleano_consulta:
                return True, None
            elif boleano_consulta:
                id_usuario = resultados_persona[0][0] if resultados_persona else None
                return False, id_usuario
            elif resultados_usuarios:
                return True, None
            else:
                return False, None, resultados_persona, resultados_usuarios
    except Exception as e:
        # Manejo de excepciones
        return False, str(e), None, None
     
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
    
    
    
    

def create_usuarios():
    try:
        nombre_tabla = 'tbl_usuarios'
        schemas = 'sch_vitale'
        metadata = MetaData()
        tabla = Table(nombre_tabla, metadata, schema=schemas, autoload_with=engine)
        consulta_insercion = insert(tabla).values(
            email = 'ibio.escobar@arus.com.coo',
            nombre ='ibio antonio escobar',
            estado = True,
            fecha_conexion = datetime.datetime.now(),
            conectado = True,)
        with engine.connect() as connection:
            connection.execute(consulta_insercion)
            connection.commit()
    # Agrega más campos y valores según la estructura de tu tabla

        # cosnulta , bool_consulta  = consulta_registro(identificacion_pacientes, False)
        # id_usuario = None
        # if not cosnulta:
        #     to_sql(name=nombre_tabla,schema=schemas,con=motor_sqlalchemy, index=False, if_exists='append')
        #     cosnulta, id_usuario= consulta_registro(identificacion_pacientes, True)
        # conexion.close()
        return  consulta_insercion
    except exc.SQLAlchemyError as err:
        print('001: Error de ingreso pacientes: '+ str(err))
        # logging.error(f"Error de ingreso pacientes: {str(err)}")
        # raise HTTPException(status_code=400,
        #                     detail={'Error':True,
        #                             'Message':'Error de ingreso pacientes',
        #                             'Result':''})
        

if __name__ == "__main__":
    # consulta_registro(usuario='ibio.escobar@arus.com.co')
    # update_cita(1,"I")
    # pr()
    create_usuarios()
    
    
# SELECT *
# FROM tbl_pacientes AS p
# JOIN tbl_municipios AS m ON p.id_municipio = m.codigo
# JOIN tbl_departamentos AS d ON m.id_depto = d.codigo
# JOIN tbl_tipos_identificacion AS i ON p.id_tipo_identificacion = i.ID AS id;


# SELECT *
# FROM tbl_pacientes AS p
# JOIN tbl_municipios AS m ON p.id_municipio = m.codigo
# JOIN tbl_departamentos AS d ON m.id_depto = d.codigo
# JOIN tbl_tipos_identificacion AS ti ON p.id_tipo_identificacion = ti.ID;
