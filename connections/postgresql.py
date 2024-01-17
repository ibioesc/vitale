import psycopg2
from sqlalchemy import create_engine,Table,MetaData, select, update, insert
import pandas as pd
from fastapi import HTTPException
import logging
import datetime


conexion_parametros = {
    'host': 'localhost',
    'port': '5432',
    'database': 'db_vitale',
    'user': 'postgres',
    'password': 'mysecretpassword'
}
# conexion_parametros = {
#     'host': '10.0.1.70',
#     'port': '5432',
#     'database': 'db_vitale',
#     'user': 'vitale',
#     'password': 'V1t4l34p1'
# }

# # sas
  
conexion = psycopg2.connect(**conexion_parametros)
motor_sqlalchemy = create_engine(f'postgresql+psycopg2://{conexion_parametros["user"]}:{conexion_parametros["password"]}@{conexion_parametros["host"]}:{conexion_parametros["port"]}/{conexion_parametros["database"]}')


def consulta_registro(identificacion = None, boleano_consulta = None, usuario = None):
    try:

        metadata = MetaData()
        # nombre_tabla =  Table('tbl_pacientes', metadata, autoload_with=motor_sqlalchemy)
        nombre_tabla =  Table('tbl_pacientes', metadata, autoload_with=motor_sqlalchemy, schema='sch_vitale')
        tabla_usuarios =  Table('tbl_usuarios', metadata, autoload_with=motor_sqlalchemy, schema='sch_vitale')
        campo_deseado = nombre_tabla.c.identificacion  # Reemplaza 'edad' con el nombre real de tu campo
        campo_email = tabla_usuarios.c.email  # Reemplaza 'edad' con el nombre real de tu campo
        condicion_where = campo_deseado == identificacion
        condicion_usuarios = campo_email == usuario
        with motor_sqlalchemy.connect() as connection:
            # Realizar una consulta para seleccionar solo el campo deseado
            consulta = nombre_tabla.select().where(condicion_where)
            consulta_persona = nombre_tabla.select()
            consulta_usuarios = tabla_usuarios.select().where(condicion_usuarios)

            # Ejecutar la consulta y recuperar los resultados
            resultados = connection.execute(consulta).fetchall()
            resultados_persona = connection.execute(consulta_persona).fetchall()
            resultados_usuarios = connection.execute(consulta_usuarios).fetchall()



            
            if   len(resultados) >=1  and boleano_consulta != True:
                return True, None,None,None
            elif boleano_consulta:
                for resultado in resultados:
                    id_usuario = resultado[0]
                return False, id_usuario,None,None
            elif resultados_usuarios:
                return False, None,resultados_persona,True

            else:
             return False, None,resultados_persona,False
    except Exception as err:
        print('004: Error de consulta: '+err)
        logging.error(f"Error de consulta registro: {str(err)}")
        raise HTTPException(status_code=400,
                            detail={'Error':True,
                                    'Message':'Error de consulta registro',
                                    'Result':''})
def consulta_tipo_identificacion(tipo_id):
    try:

        metadata = MetaData()
        # nombre_tabla =  Table('tbl_pacientes', metadata, autoload_with=motor_sqlalchemy)
        nombre_tabla =  Table('tbl_tipos_identificacion', metadata, autoload_with=motor_sqlalchemy, schema='sch_vitale')
        campo_deseado = nombre_tabla.c.abreviacion  # Reemplaza 'edad' con el nombre real de tu campo
        condicion_where = campo_deseado == tipo_id

        with motor_sqlalchemy.connect() as connection:
            # Realizar una consulta para seleccionar solo el campo deseado
            consulta = nombre_tabla.select().where(condicion_where)

            # Ejecutar la consulta y recuperar los resultados
            resultados = connection.execute(consulta).fetchall()

            for resultado in resultados:
                    id_usuario = resultado[0]
            return id_usuario

    except Exception as err:
        print('004: Error de consulta: '+err)
        logging.error(f"Error de consulta registro: {str(err)}")
        raise HTTPException(status_code=400,
                            detail={'Error':True,
                                    'Message':'Error de consulta registro',
                                    'Result':''})



def create_pacientes(df):
    try:
        nombre_tabla = 'tbl_pacientes'
        for dfp in df:
            daf = pd.DataFrame([dfp])
            identificacion_pacientes =  daf['identificacion']
            cosnulta , bool_consulta  = consulta_registro(identificacion_pacientes, False)
            id_usuario = None
            if not cosnulta:
                daf.to_sql(name=nombre_tabla,schema='sch_vitale' ,con=motor_sqlalchemy, index=False, if_exists='append')
                cosnulta, id_usuario= consulta_registro(identificacion_pacientes, True)
        conexion.close()
        return cosnulta,id_usuario
    except Exception as err:
        print('001: Error de ingreso pacientes: '+ str(err))
        logging.error(f"Error de ingreso pacientes: {str(err)}")
        raise HTTPException(status_code=400,
                            detail={'Error':True,
                                    'Message':'Error de ingreso pacientes',
                                    'Result':''})

def create_usuarios(email):
    try:
        nombre_tabla = 'tbl_usuarios'
        nombre_permisos_usuarios = 'tbl_permisos_usuarios'
        schemas = 'sch_vitale'
        metadata = MetaData()
        tabla = Table(nombre_tabla, metadata, schema=schemas, autoload_with=motor_sqlalchemy)
        tabla_permiso_usuarios = Table(nombre_permisos_usuarios, metadata, schema=schemas, autoload_with=motor_sqlalchemy)
        email = 'cesar.rojas@arus.com.co'
        update_usuario = tabla.c.email == email
        _,_,_,cosnulta  = consulta_registro(usuario=email)
        if not cosnulta:
            consulta_insercion = insert(tabla).values(
                email = email,
                nombre ='ibio antonio escobar',
                estado = True,
                fecha_conexion = datetime.datetime.now(),
                conectado = True,)
            insercion_permisos_usuarios = insert(tabla_permiso_usuarios).values(id_usuario=email,id_permiso='1')
            with motor_sqlalchemy.connect() as connection:
                connection.execute(consulta_insercion)
                connection.execute(insercion_permisos_usuarios)
                connection.commit()
        #metodo actualizar
        if cosnulta:
            consulta_actualizacion = update(tabla).where(update_usuario).values(fecha_conexion = datetime.datetime.now())
            with motor_sqlalchemy.connect() as connection:
                connection.execute(consulta_actualizacion)
                connection.commit()
            
        return cosnulta
    except Exception as err:
        print('001: Error de ingreso pacientes: '+ str(err))
        logging.error(f"Error de ingreso pacientes: {str(err)}")
        raise HTTPException(status_code=400,
                            detail={'Error':True,
                                    'Message':'Error de ingreso pacientes',
                                    'Result':''})

        
        
def create_pacientes_upload(df,dfcitas):
    try:
        nombre_tabla = 'tbl_pacientes'
        for _ in df:

            identificacion_pacientes =  df['identificacion']
            df['barrio'] = 'bello'
            df['id_municipio'] = '05088'
            
            for contador, idpaciente in enumerate(identificacion_pacientes):
                idtipo = df['id_tipo_identificacion'][contador]
                cosnulta_tipo_id = consulta_tipo_identificacion(idtipo)
                cosnulta , bool_consulta ,_,_ = consulta_registro(str(idpaciente), False)
                # cosnulta = False
                # idpaciente = '8723673273423267913'
                # df['identificacion'] = '8723673273423267913'
                id_usuario = None
                df['id_tipo_identificacion'] = 2
                df['estado'] = True
                if  cosnulta:
                    
                    cosnulta, id_usuario,_,_= consulta_registro(str(idpaciente), True)
                    # create_citas_upload(dfcitas,bool_consulta,id_usuario)
                else:
                    df.to_sql(name=nombre_tabla,schema='sch_vitale' ,con=motor_sqlalchemy, index=False, if_exists='append')
                    cosnulta, id_usuario,_,_= consulta_registro(str(idpaciente), True)
                    create_citas_upload(dfcitas,bool_consulta,id_usuario)
        conexion.close()
        return cosnulta,id_usuario
    except Exception as err:
        print('001: Error ingresar pacientes upload: '+ str(err))
        logging.error(f"Error ingresar pacientes upload: {str(err)}")
        raise HTTPException(status_code=400,
                            detail={'Error':True,
                                    'Message':'Error ingresar pacientes upload',
                                    'Result':''})


def create_citas_upload(df,usuario_existe, id_usuario):
    try:
        nombre_tabla = 'tbl_citas'
        df['id_paciente'] = id_usuario
        df['hora'] = '2023-10-26'
        df['estado'] = 'M'
        if id_usuario:    
                df.to_sql(name=nombre_tabla,schema='sch_vitale' ,con=motor_sqlalchemy, index=False, if_exists='append')
        conexion.close()
    except Exception as err:
        print('003: Error ingresar citas: '+ str(err))
        logging.error(f"Error ingresar citas upload: {str(err)}")
        raise HTTPException(status_code=400,
                            detail={'Error':True,
                                    'Message':'Error ingresar citas upload',
                                    'Result':''})
        
        
def create_citas(df,usuario_existe, id_usuario):
    try:
        nombre_tabla = 'tbl_citas'
        df[0]['id_paciente'] = id_usuario 
        for dfp in df:
            daf = pd.DataFrame([dfp])
            if not usuario_existe:    
                daf.to_sql(name=nombre_tabla,schema='sch_vitale' ,con=motor_sqlalchemy, index=False, if_exists='append')
        conexion.close()
    except Exception as err:
        print('003: Error ingresar citas: '+ str(err))
        logging.error(f"Error crear citas : {str(err)}")
        raise HTTPException(status_code=400,
                            detail={'Error':True,
                                    'Message':'Error crear citas',
                                    'Result':''})

    
    
    
def create_personas(df,usuario_existe):
    try:
        nombre_tabla = 'tbl_personas'
        for dfp in df:
            daf = pd.DataFrame([dfp])
            if not usuario_existe:
                daf.to_sql(name=nombre_tabla,schema='sch_vitale' ,con=motor_sqlalchemy, index=False, if_exists='append')
        conexion.close()
    except Exception as err:
        logging.error(f"Error crear citas : {str(err)}")
        raise HTTPException(status_code=400,
                            detail={'Error':True,
                                    'Message':'Error crear citas',
                                    'Result':''})
        
def consulta_paciente(email = None):
    try:
        metadata = MetaData()
        tbl_pacientes = Table('tbl_pacientes', metadata, autoload_with=motor_sqlalchemy,schema='sch_vitale')
        tbl_citas = Table('tbl_citas', metadata, autoload_with=motor_sqlalchemy,schema='sch_vitale')
        tbl_permisos_usuarios = Table('tbl_permisos_usuarios', metadata, autoload_with=motor_sqlalchemy,schema='sch_vitale')
        tbl_usuarios = Table('tbl_usuarios', metadata, autoload_with=motor_sqlalchemy,schema='sch_vitale')
        tabla_municipios = Table('tbl_municipios', metadata, autoload_with=motor_sqlalchemy,schema='sch_vitale')
        tabla_departamentos = Table('tbl_departamentos', metadata, autoload_with=motor_sqlalchemy,schema='sch_vitale')
        tabla_tipos_identificacion = Table('tbl_tipos_identificacion', metadata, autoload_with=motor_sqlalchemy,schema='sch_vitale')


        with motor_sqlalchemy.connect() as connection:
            if email:
                
            #consulta = select(tbl_permisos_usuarios, tbl_usuarios).select_from(tbl_permisos_usuarios.join(tbl_usuarios, tbl_permisos_usuarios.c.id_usuario == tbl_usuarios.c.email))
            #consulta = select(tbl_permisos_usuarios, tbl_usuarios).select_from(tbl_permisos_usuarios.join(tbl_usuarios, tbl_permisos_usuarios.c.id_usuario == tbl_usuarios.c.email)).where(tbl_usuarios.c.email == email)
                consulta = select(tbl_permisos_usuarios, tbl_usuarios).select_from(tbl_permisos_usuarios.join(tbl_usuarios, tbl_permisos_usuarios.c.id_usuario == tbl_usuarios.c.email)).where(tbl_usuarios.c.email == email)
            else:
                consulta = select(
                            tbl_pacientes,
                            tabla_municipios,
                            tabla_departamentos,
                            tabla_tipos_identificacion
                ).select_from(tbl_pacientes.join(tabla_municipios, tbl_pacientes.c.id_municipio == tabla_municipios.c.codigo)
                            .join(tabla_departamentos, tabla_municipios.c.id_depto == tabla_departamentos.c.codigo)
                            .join(tabla_tipos_identificacion, tbl_pacientes.c.id_tipo_identificacion == tabla_tipos_identificacion.c.ID)
                )
                # consulta = select(tbl_pacientes, tbl_citas).select_from(tbl_pacientes.join(tbl_citas, tbl_pacientes.c.ID == tbl_citas.c.ID))
            resultados = connection.execute(consulta)
            nombres_columnas = resultados.keys()
            resultados_json = []
            for resultado in resultados:
                fila_dict = {nombre_columna: valor for nombre_columna, valor in zip(nombres_columnas, resultado)}
                resultados_json.append(fila_dict)
        resultados_dic = {
                "Status": 200,
                "Error": None,
                "Message": "",
                "Result":resultados_json
                    }   
        return resultados_dic
    except (Exception , TypeError) as err:
        print('004: Error de consulta: '+str(err))
        logging.error(f"Error de consulta: {str(err)}")
        raise HTTPException(status_code=400,
                            detail={'Error':True,
                                    'Message':'Error de consulta',
                                    'Result':''})

   
        
def update_cita(id_cita :str , estado_cita :str, observacion:str):
    try:
        cur = conexion.cursor()
        # Ejecutamos una consulta
        cur.execute( f'''UPDATE tbl_citas SET estado = '{estado_cita}', observacion = '{observacion}'  WHERE "ID"  = {id_cita};''' )
        updated_rows = cur.rowcount
        conexion.commit()
        cur.close()
        resultados_dic = {
                "Status": 200,
                "Error": None,
                "Message": 'Actualizado correctamente',
                "Result":''
                    }  
        return resultados_dic

    except (Exception , TypeError) as err:
        print('004: Error de actualizar: '+str(err))
        raise HTTPException(status_code=400,
                            detail={'Error':True,
                                    'Message':'Error de Actualizar',
                                    'Result':''})
    