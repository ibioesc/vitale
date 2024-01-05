import logging

from ldap3 import ALL, ALL_ATTRIBUTES, SUBTREE, Connection, Server

logger = logging.getLogger('pyLdap')

SERVER_LDAP = 'ldaps://10.0.1.230:636'


def authenticateLdapUser (user, password):
    server = Server(SERVER_LDAP, get_info=ALL)
    conn = Connection(server, user, password)
    conn.open()
    if conn.bind():
        return True
    else:
        return False
    
    
    
if __name__ == '__main__':
    pass