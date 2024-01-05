import psycopg2
from sqlalchemy import create_engine,Table,MetaData, select, update
import pandas as pd
from decouple import config


# conexion_parametros = {
#     'host': 'localhost',
#     'port': '5432',
#     'database': 'db_vitale',
#     'user': 'postgres',
#     'password': 'mysecretpassword'
# }
conexion_parametros = {
    'host': '10.0.1.70',
    'port': '5432',
    'database': 'db_vitale',
    'user': 'postgres',
    'password': 'V1t4l34p1'
}

# sas
  
conexion = psycopg2.connect(**conexion_parametros)
motor_sqlalchemy = create_engine(f'postgresql+psycopg2://{conexion_parametros["user"]}:{conexion_parametros["password"]}@{conexion_parametros["host"]}:{conexion_parametros["port"]}/{conexion_parametros["database"]}')


def consulta_registro(identificacion=None, boleano_consulta=None):
    try:
        metadata = MetaData()
        nombre_tabla =  Table('tbl_pacientes', metadata, autoload_with=motor_sqlalchemy)
        campo_deseado = nombre_tabla.c.identificacion  # Reemplaza 'edad' con el nombre real de tu campo
        condicion_where = campo_deseado == identificacion
        with motor_sqlalchemy.connect() as connection:
            # Realizar una consulta para seleccionar solo el campo deseado
            consulta = nombre_tabla.select().where(condicion_where)
            consulta_persona = nombre_tabla.select()

            # Ejecutar la consulta y recuperar los resultados
            resultados = connection.execute(consulta).fetchall()
            resultados_persona = connection.execute(consulta_persona).fetchall()


            
            if   len(resultados) >=1  and boleano_consulta != True:
                return True, None,
            elif boleano_consulta:
                for resultado in resultados:
                    id_usuario = resultado[0]
                return False, id_usuario
            else:
             return False, None,resultados_persona
    except Exception as err:
        print('004: Error de consulta: '+err)


def create_pacientes(df):
    try:
        nombre_tabla = 'tbl_pacientes'
        for dfp in df:
            daf = pd.DataFrame([dfp])
            identificacion_pacientes =  daf['identificacion']
            cosnulta , bool_consulta  = consulta_registro(identificacion_pacientes, False)
            id_usuario = None
            if not cosnulta:
                daf.to_sql(nombre_tabla, con=motor_sqlalchemy, index=False, if_exists='append')
                cosnulta, id_usuario= consulta_registro(identificacion_pacientes, True)
        conexion.close()
        return cosnulta,id_usuario
    except Exception as er:
        print('001: Error ingresar pacientes: '+ str(er))
        
        
def create_pacientes_upload(df,dfcitas):
    try:
        nombre_tabla = 'tbl_pacientes'
        for _ in df:

            identificacion_pacientes =  df['identificacion']
            df['barrio'] = 'bello'
            df['depto'] = 'bello'
            for idpaciente in identificacion_pacientes:
                cosnulta , bool_consulta  = consulta_registro(str(idpaciente), False)
                # cosnulta = False
                # idpaciente = '8723673273423267913'
                # df['identificacion'] = '8723673273423267913'
                id_usuario = None
                if not cosnulta:
                    df.to_sql(nombre_tabla, con=motor_sqlalchemy, index=False, if_exists='append')
                    cosnulta, id_usuario= consulta_registro(str(idpaciente), True)
                create_citas_upload(dfcitas,bool_consulta,id_usuario)
        conexion.close()
        return cosnulta,id_usuario
    except Exception as er:
        print('001: Error ingresar pacientes: '+ str(er))

def create_citas_upload(df,usuario_existe, id_usuario):
    try:
        nombre_tabla = 'tbl_citas'
        df['id_paciente'] = id_usuario
        df['hora'] = '2023-10-26'
        if id_usuario:    
                df.to_sql(nombre_tabla, con=motor_sqlalchemy, index=False, if_exists='append')
        conexion.close()
    except Exception as er:
        print('003: Error ingresar citas: '+ str(er))
        
        
def create_citas(df,usuario_existe, id_usuario):
    try:
        nombre_tabla = 'tbl_citas'
        df[0]['id_paciente'] = id_usuario 
        for dfp in df:
            daf = pd.DataFrame([dfp])
            if not usuario_existe:    
                daf.to_sql(nombre_tabla, con=motor_sqlalchemy, index=False, if_exists='append')
        conexion.close()
    except Exception as er:
        print('003: Error ingresar citas: '+ str(er))

    
    
    
def create_personas(df,usuario_existe):
    try:
        nombre_tabla = 'tbl_personas'
        for dfp in df:
            daf = pd.DataFrame([dfp])
            if not usuario_existe:
                daf.to_sql(nombre_tabla, con=motor_sqlalchemy, index=False, if_exists='append')
        conexion.close()
    except Exception as er:
        print('002: Error ingresar personas: '+ str(er))
        
def consulta_paciente():
    try:
        metadata = MetaData()
        tbl_pacientes = Table('tbl_pacientes', metadata, autoload_with=motor_sqlalchemy)
        tbl_citas = Table('tbl_citas', metadata, autoload_with=motor_sqlalchemy)

        with motor_sqlalchemy.connect() as connection:
            consulta = select(tbl_pacientes, tbl_citas).select_from(tbl_pacientes.join(tbl_citas, tbl_pacientes.c.ID == tbl_citas.c.ID))
            resultados = connection.execute(consulta)
            nombres_columnas = resultados.keys()
            resultados_json = []
            for resultado in resultados:
                fila_dict = {nombre_columna: valor for nombre_columna, valor in zip(nombres_columnas, resultado)}
                resultados_json.append(fila_dict)
        resultados_dic = {
                "status": 200,
                "error": None,
                "message": "",
                "result":resultados_json
                    }   
        return resultados_dic
    except (Exception , TypeError) as err:
        print('004: Error de consulta: '+str(err))
        resultados_dic = {
                "status": 401,
                "error": str(err),
                "message": "Error de consulta",
                "result":''
                    }   
        
def update_cita(id_cita :str , estado_cita :str, observacion:str):
    try:
        cur = conexion.cursor()
        # Ejecutamos una consulta
        cur.execute( f'''UPDATE tbl_citas SET estado = '{estado_cita}', observacion = '{observacion}'  WHERE "ID"  = {id_cita};''' )
        updated_rows = cur.rowcount
        conexion.commit()
        cur.close()
        resultados_dic = {
                "status": 200,
                "error": None,
                "message": 'Actualizado correctamente',
                "result":''
                    }  
        return resultados_dic

    except (Exception , TypeError) as err:
        print('004: Error de actualizar: '+str(err))
        resultados_dic = {
                "status": 404,
                "error": str(err),
                "message": "Error de actualizar",
                "result":''
                    } 
        return  resultados_dic
        
    