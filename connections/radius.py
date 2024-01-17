
from pyrad.client import Client
from pyrad.dictionary import Dictionary
import pyrad.packet

def radiu(usuario,contraseña,clien_secret):
    


    srv = Client(server="10.0.0.203", secret=b"V1T4L_3sr123", dict=Dictionary("dictionary"))
    # create request
    req = srv.CreateAuthPacket(code=pyrad.packet.AccessRequest,User_Name=usuario)
    req["User-Password"] = req.PwCrypt(contraseña+clien_secret)

    try:
        print("Sending authentication request")
        reply = srv.SendPacket(req)
    except pyrad.client.Timeout:
        print("RADIUS server does not reply")

    if reply.code == pyrad.packet.AccessAccept:
        print("Access accepted")
    else:
        print("Access denied")

    print("Attributes returned by server:")
    for i in reply.keys():
        print("%s: %s" % (i, reply[i]))
    # srv = Client(server="10.0.0.203", secret=b"V1T4L_3sr123"
    #             ,dict=Dictionary("dictionary"))



    # # send request
    # reply = srv.SendPacket(req)

    # if reply.code == pyrad.packet.AccessAccept:
    #     print("access accepted")
    # else:
    #     print("access denied")

    # print("Attributes returned by server:")
    # for i in reply.keys():
    #     print("%s: %s" % (i, reply[i]))