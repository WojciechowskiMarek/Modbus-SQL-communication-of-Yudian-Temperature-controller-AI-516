import pypyodbc as odbc
from pymodbus.client.serial import ModbusSerialClient as ModbusClient
import datetime
import time
import re

DRIVER_NAME = "SQL SERVER"
SERVER_NAME = "Your server"
DATABASE_NAME = "Your database name"

connection_string = f"""
    DRIVER={{{DRIVER_NAME}}};
    SERVER={{{SERVER_NAME}}};
    DATABASE={{{DATABASE_NAME}}};
    Trust_Connection=yes;
"""

client = ModbusClient(port="COM3",stopbits = 1, bytesize = 8, parity = 'N', baudrate= 9600)
conn = odbc.connect(connection_string)
cursor = conn.cursor()
cursor.execute("select max(id) from Temperature;") # check latest ID in sql base to set new unique id
result = cursor.fetchone()    
Last_Max_ID_SQL = re.sub(r"\D", "", str(result))
print (Last_Max_ID_SQL, " number of last saved record")
if len (Last_Max_ID_SQL) == 0: Last_Max_ID_SQL = 0 # if base empty id = 0
ID_Value = int(Last_Max_ID_SQL) + 1
while True:
    PV = client.read_holding_registers(0x4a,1,0x01) #PV value
    SV = client.read_holding_registers(0x4b,1,0x01) #SV value
    SP1 = client.read_holding_registers(0x00,1,0x01) #SP1 given value
    HIAL = client.read_holding_registers(0x01,1,0x01) # HIAL upper limit alarm
    LOAL = client.read_holding_registers(0x02,1,0x01) # LOAL upper limit alarm
    DIAG = client.read_holding_registers(0xe8,2,0x01) # DIAG register
    a = (PV.registers[0]/10)
    b = (SV.registers[0]/10)
    c = (HIAL.registers[0]/10)
    d = (SP1.registers[0]/10)
    e = (LOAL.registers[0]/10)
    f = (DIAG.registers[0],DIAG.registers[1])
    x = datetime.datetime.now()
    timee = x.strftime("%X")
    date = x.strftime("%x")    
    cursor.execute("insert into Temperature(ID,PV,SV,HIAL,SP1,LOAL,date,timee) values (?,?,?,?,?,?,?,?)",(ID_Value,a,b,c,d,e,date,timee))
    cursor.commit()
    print (ID_Value,a,b,c,d,e,date,timee)
    ID_Value = ID_Value + 1
    time.sleep(5)
