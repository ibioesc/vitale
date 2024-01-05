from pyfortiapi import FortiGateAPI
import pyfortiapi




# Configuración de la conexión FortiGate
def fortin():
    fortigate_ip = 'tu_direccion_ip_del_FortiGate'
    username = 'tu_usuario'
    password = 'tu_contraseña'

    # Configuración de la conexión VPN
    vpn_gateway = '10.0.58.71'
    vpn_username = 'ibio.escobar'
    vpn_password = ''

    # Iniciar sesión 'en FortiGate
    with FortiGateAPI(fortigate_ip, username, password) as api:
        # Conectar o descone'ctar la VPN
        api.vpn.ssl.status(vpn='SSLVPN_TUNNEL', vdom='root', name='SSLVPN_TUNNEL_NAME', action='connect', user=vpn_username, password=vpn_password)
    # device = pyfortiapi.FortiGate(ipaddr=vpn_gateway,
    #                               username=vpn_username,
    #                               password=vpn_password)
    # print(device)



if __name__ == "__main__":
    fortin()