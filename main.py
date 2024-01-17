import uvicorn
from datetime import datetime, timedelta
from typing import Annotated, Union
from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError, jwt
from passlib.context import CryptContext
from pydantic import BaseModel
from .connections.directoryActive import authenticateLdapUser
import base64
import pandas as pd
from .direcciones import test_API
from .connections.postgresql import consulta_paciente,consulta_registro,create_pacientes_upload,update_cita,create_usuarios,consulta_tipo_identificacion
# from .sura import pruebas_datos
from fastapi.middleware.cors import CORSMiddleware
from fastapi_jwt_auth import AuthJWT
import csv
import os
import logging
from connections.radius import radiu





# to get a string like this run:
# openssl rand -hex 32
SECRET_KEY = "DuPxSHZzrlCBAHgnMmfvx5ehgVamIcDmtCxunmEWx8I="
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 5



class Token(BaseModel):
    access_token: str
    token_type: str
    respues_usuario :str



class update_app(BaseModel):
    id_cita: int
    estado_cita: str
    observacion: str



class TokenData(BaseModel):
    username: Union[str, None] = None


# class User(BaseModel):
#     username: str
#     email: Union[str, None] = None
#     full_name: Union[str, None] = None
#     disabled: Union[bool, None] = None
    
class Upload(BaseModel):
    base6: str


# class UserInDB(User):
#     hashed_password: str


pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

app = FastAPI()

logging.basicConfig(
    filename='app.log',  # Puedes cambiar el nombre del archivo según tus preferencias
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO  # Puedes cambiar el nivel de registro según tus necesidades (INFO, DEBUG, ERROR, etc.)
)

origins = [
    "http://localhost:4200",
    "http://localhost:8080",
    "http://localhost:8000"
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# def verify_password(plain_password, hashed_password):
#     return pwd_context.verify(plain_password, hashed_password)

def credentials_exception ():
        credentials_excepti = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
        logging.error(f"Could not validate credentials")
        return credentials_excepti


def get_password_hash(password):
    return pwd_context.hash(password)


# def get_user(db, username: str):
#     if username in db:
#         user_dict = db[username]
#         return UserInDB(**user_dict)


# def authenticate_user(fake_db, username: str, password: str):
#     user = get_user(fake_db, username)
#     if not user:
#         return False
#     if not verify_password(password, user.hashed_password):
#         return False
#     return user

# def create_refresh_token(data: dict, expires_delta: timedelta):
#     to_encode = data.copy()
#     expire = datetime.utcnow() + expires_delta
#     to_encode.update({"exp": expire})
#     refresh_token = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
#     return refresh_token


def create_access_token(data: dict, expires_delta: Union[timedelta, None] = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=5)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


async def get_current_user(upload : Upload,token: Annotated[str, Depends(oauth2_scheme)]):

    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception()
        _ = TokenData(username=username)
        base_64 = base64.b64decode(upload.base6)
        with open('./document/mymessage.xls', 'wb') as f:
            f.write(bytes(base_64))
        df = pd.read_excel('./document/mymessage.xls')
        df = test_API(df)
        df = df.rename(columns={'Zona':'zona',
                                'Tipo Identificación Paciente':'id_tipo_identificacion',
                                'Identificación Paciente':'identificacion',
                                'Nombre Paciente':'nombre',
                                'Plan de Salud':'plan_salud',
                                            'Direccion Paciente':'direccion_original',
                                            'Teléfono Paciente': 'telefono',
                                            'Piso Paciente':'piso',
                                            'Municipio':'id_municipio',
                                            'Medicamento':'medicamento',
                                            'Fecha de nacimiento paciente':'fecha_nace',
                                            'Sexo paciente':'genero',
                                            'Tratamiento':'tratamiento',
                                            'Fecha Inicio Cita':'fecha'
                                            })   
        columna_paciente = [
            'id_tipo_identificacion',
            'identificacion',
            'direccion_original',
            'direccion',
            'telefono',
            'zona',
            'piso',
            'plan_salud',
            'tratamiento',
            'id_municipio',
            'nombre',
            'medicamento',
            'fecha_nace',
            'genero'
            ]
        columna_cita = [
            'fecha'        
        ]
        df_create_paciente = df[columna_paciente].copy()
        df_create_cita = df[columna_cita].copy()
        create_pacientes_upload(df_create_paciente,df_create_cita)
        return {'Respuesta':'Archivo procesado'}
    except JWTError:
        raise credentials_exception()
    # user = get_user(fake_users_db, username=token_data.username)
    # if user is None:
    #     raise credentials_exception
    



async def get_current_active_user(
    current_user: Annotated[Upload, Depends(get_current_user)] 
):
    # if current_user.disabled:
    #     raise HTTPException(status_code=400, detail="Inactive user")
    return current_user

@app.get("/patient")
async def get_current_paciente(token: Annotated[str, Depends(oauth2_scheme)] 
):
    # execption = credentials_exception()

    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception()
        paciente = consulta_paciente()

    # if current_user.disabled:
    #     raise HTTPException(status_code=400, detail="Inactive user")
        return paciente
    except JWTError:
        raise credentials_exception()


@app.post("/token", response_model=Token)
async def login_for_access_token(
    form_data: Annotated[OAuth2PasswordRequestForm, Depends()]
):
    # user = authenticate_user(fake_users_db, form_data.username, form_data.password)
    
    # user = authenticateLdapUser(form_data.username, form_data.password)
    
    user = radiu(form_data.username, form_data.password, form_data.client_secret)
    logging.info(f"El usuario {form_data.username} accedio a la ruta principal")
    _ = create_usuarios(form_data.username)
    password = get_password_hash(form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Usuario o Contraseña incorrecta",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub":form_data.username}, expires_delta=access_token_expires
    )
    
    
    respues_usuario = consulta_paciente('ibio.escobar@arus.com.co')
    res = {"access_token": access_token, "token_type": "bearer", "respues_usuario": str(respues_usuario)}
    return res


@app.post("/upload")
async def read_users_me(
    current_user: Annotated[Upload, Depends(get_current_active_user )]
):
    return current_user

@app.get("/refresh_token")
async def refresh_token(token: Annotated[str, Depends(oauth2_scheme)]):

    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception()
    except JWTError:
        raise credentials_exception()
    
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(data={"sub": username}, expires_delta=access_token_expires)
    return {"access_token": access_token, "token_type": "bearer"}


@app.put("/update_appointment")

async def updateAppointment(upda: update_app, token: Annotated[str, Depends(oauth2_scheme)]):

    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception()
        resul_cita = update_cita(upda.id_cita, upda.estado_cita, upda.observacion)
        return resul_cita
    except JWTError:
        raise credentials_exception()


@app.get("/report/pdi")
async def expor_file(token: Annotated[str, Depends(oauth2_scheme)]):
    import datetime

    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception()
        _,_,resultados_persona,_ = consulta_registro()
        list_resul = []
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
        nombre_carpeta2 = nombre_carpeta.strftime("%d-%m-%Y,%H,%M,%S")
        nombre_carpeta = './documentCSV/'+str(nombre_carpeta2)
        os.mkdir(str(nombre_carpeta))
        with open(f'{nombre_carpeta}/puntos.csv', mode='w',encoding="utf-8") as file:
            writer = csv.DictWriter(file, delimiter=',', fieldnames=columns )
            writer.writeheader()
            for course in list_resul:
                writer.writerow(course)

        return{'result':'/opt/vitale/'+nombre_carpeta2}
        
    except JWTError:
        raise credentials_exception()
    except Exception as ar:
        print(ar)


# if __name__ == "__main__":
#     uvicorn.run(app, host="127.0.0.1", port=8080)