from fastapi import FastAPI, APIRouter, UploadFile
import pandas as pd
import numpy as np
import re
from .nomenclaturas import *
import geocoder

# router = APIRouter()

# @router.get("/test", 
#     tags=["Conexión"],        
#     summary="Método para verificar la conexión con la API",dirs[0]['new']
# )
def test_API(df):
    # df = pd.read_excel(file)
    # df = df.replace(np.nan,'', regex=True)
    dirs = []
    for index, data in df.iterrows():
        direccion = str(data['Direccion Paciente']).upper()
        if  direccion:
            dir = formatAddress(direccion,reemplazantes)
            dir = mappersAddress(dir)
            dir += ', '+ str(data['Municipio']).upper() +', ANTIOQUIA'
            g = geocoder.arcgis(dir)
            gosm = geocoder.osm(dir)
    #         print(g.latlng)
    #         print(gosm.latlng)
    #         dirs.append(dir)
    df['direccion'] = dir        

    return df

# mapeo la direccion al formato 
# [tipo via|numero|letra opcional|cuadrante opcional|# opcional|numero|letra opcional
# |cuadrante opcional|numero final] 
def mappersAddress(dire:str):
    split = dire.split(' ')
    if  split.__len__() < 4:
        return dire
    
    dire = ''
    tipo_via = split[0]

    #   Comienzo con evaluar los tipos de vías. y Sigue un espacio.
    if  tipo_via in tipos_vias:
        dire = tipo_via + ' '
    
    #   Se hace loop para validar las direcciones
    if  split.__len__() > 1:
        for index, x in enumerate(split):
            if  index > 0:
                dire = loopValidate(split,index,dire)
                if  validateAddress(dire):
                    break
    return dire

# Formateo la dirección y reemplazo para estandarizar tipos de vias 
def formatAddress(dire:str, reemplaces: []):
    for dir in reemplaces:
        dire = dire.replace(dir["search"],dir["value"])

    return dire

# Valido si la dirección tiene el formato
def validateAddress(dire:str):
    valid = False
    tipo_via = (dire.startswith("CL") or
                dire.startswith("CR") or
                dire.startswith("AVENIDA") or
                dire.startswith("DIAGONAL") or
                dire.startswith("CIRCULAR") or
                dire.startswith("TRANSVERSAL"))
    numero = re.findall("(?<=[a-z# -])\d+", dire).__len__() == 3
    if  (numero and 
        tipo_via):
        valid = True
    return valid

# Evaluo cada campo del split, si es una letra o # no se le aplica el espacio 
def loopValidate(split:[], index: int, dire:str):
    if  split.__len__() > index:
        text = split[index]
        if  split.__len__() > index+1:
            text_ = split[index+1]
        else :
            text_ = text
        if  (text not in tipos_vias or
            text in cuadrantes or 
            text in prefijos or 
            re.search('^[#]', text, flags=re.I) or 
            re.search('^[0-9]', text, flags=re.I)):
            if  (text == "#" or 
                 (re.search('^[0-9]|[0-9]$', text, flags=re.I) 
                  and text_ in prefijos)):
                dire += text 
            else:
                dire += text + ' '
    
    return dire


# app = FastAPI(
#     title="API de Prueba",
#     version="v0.0.1",
#     description="API de Pruebas"
# )
# app.include_router(router)