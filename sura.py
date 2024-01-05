from  connections.postgresql  import create_pacientes, create_personas, create_citas, consulta_registro
import requests
from requests.exceptions import RequestException
from requests.auth import HTTPBasicAuth

def pruebas_datos():
    
    fake_users_db = {
            "status": 200,
            "flag": True,
            "error": None,
            "message": "",
            "tecnicalMessage": "",
            "detail": "OK",
            "result": [
                {
                    "nmCita": 95625,
                    "cdTipIdeProf": "CC",
                    "dniProf": "1038816379434000",
                    "feHoraIniCita": "2023-04-18 14:56:00.000",
                    "feHoraFinCita": "2023-04-18 15:26:00.000",
                    "cdTipIdePac": "CC",
                    "dniPac": "103885447125356",
                    "dsDireccionPac": "karrera 74 # 98 35",
                    "dsTelefonoPac": "2222222 ext 22  32022222222222  ",
                    "dsBarrio": "las perlas",
                    "dsZona": "Zona 3",
                    "piso": "Piso Cronicos",
                    "feHoraIniTurno": "2023-04-18 10:00:09.000",
                    "feHoraFinTurno": "2023-04-18 19:00:00.000",
                    "dsTurno": "TN1",
                    "dsPlan": "POS",
                    "dsTratamiento": "Atención Profesionales Ingreso terapia respiratoria domiciliario ",
                    "cdMunicipio": "4292",
                    "dsMunicipio": "MEDELLIN",
                    "dsNombreProf": "USUARIO2 CERTIFICACION IPSA ",
                    "dsNombrePac": "IBIO ANTONIO ESCOBAR GOMEZ",
                    "medicamento": None,
                    "fechaNacimientoPac": "1991-04-20 00:00:00.000",
                    "codigoSexoPac": "F",
                    "dsMensajeUsuario": "",
                    "dsMensajeTenico": ""
                },

                ]
            }
    return fake_users_db


def url_api():
    try:
        url = "https://597b-201-185-232-137.ngrok.io/furapi/cita/consultar?codigoCiudad=106&feHoraIniCita=2023-04-18&feHoraFinCita=2023-04-18"
        headers = {'Content-Type': 'application/json'}
        auth = HTTPBasicAuth('implinea', 'implinea')
        response = requests.request("GET",url,auth=auth,headers=headers,verify=False)
        return response.json()
    except (ValueError, RequestException) as e:
        print(e)
        raise Exception('Invalid json: {}'.format(e)) from e




def sura_api(datos):
    list_paciente = []
    list_cita = []
    list_persona = []
    for datos_usuario in datos['result']:
        list_paciente.append({
            'tipo_identificacion' : datos_usuario['cdTipIdePac'],
            'identificacion' :datos_usuario['dniPac'],
            'direccion_original':datos_usuario['dsDireccionPac'],
            'direccion':' N/A',
            'telefono':datos_usuario['dsTelefonoPac'],
            'barrio':datos_usuario['dsBarrio'],
            'zona':datos_usuario['dsZona'],
            'piso':datos_usuario['piso'],
            'plan_salud':datos_usuario['dsPlan'],
            'tratamiento':datos_usuario['dsTratamiento'],
            'depto':datos_usuario['cdMunicipio'],
            'municipio':datos_usuario['dsMunicipio'],
            'nombre':datos_usuario['dsNombrePac'],
            'medicamento':datos_usuario['medicamento'],
            'fecha_nace':datos_usuario['fechaNacimientoPac'],
            'genero':datos_usuario['codigoSexoPac']
        })
        list_cita.append({
            'id_cita_cliente': datos_usuario['nmCita'],
            'fecha': datos_usuario['feHoraIniCita'],
            'id_paciente': 1,
            'hora' : datos_usuario['feHoraIniCita']

        })
        list_persona.append({
                'tipo_identificacion':datos_usuario['cdTipIdeProf'],
                'identificacion':datos_usuario['dniProf'],
                'nombre':datos_usuario['dsNombreProf'],
                'tipo_persona':'N/A'


        })
    return list_persona,list_cita,list_paciente


if __name__ == "__main__":
   datos = pruebas_datos()
#    datos = url_api()
   list_persona,list_cita,list_paciente = sura_api(datos)
   usuario_existe, id_paciente = create_pacientes(list_paciente)
   create_citas(list_cita,usuario_existe,id_paciente)
   create_personas(list_persona,usuario_existe)

#    consulta_registro()


# cdTipIdePac
