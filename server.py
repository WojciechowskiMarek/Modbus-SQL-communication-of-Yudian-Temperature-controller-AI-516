import pypyodbc as odbc
from pymodbus.client.serial import ModbusSerialClient as ModbusClient
import re
import time as time11
from datetime import datetime as dt
from datetime import datetime,date,time
import logging

DRIVER_NAME = "SQL SERVER"
SERVER_NAME = "PLNB027\SQLEXPRESS01"
DATABASE_NAME = "Office"

connection_string = f"""
    DRIVER={{{DRIVER_NAME}}};
    SERVER={{{SERVER_NAME}}};
    DATABASE={{{DATABASE_NAME}}};
    Trust_Connection=yes;
"""
logging.basicConfig();
log = logging.getLogger();
log.setLevel(logging.DEBUG);
client = ModbusClient(port="COM3",stopbits = 1, bytesize = 8, parity = 'N', baudrate= 9600)
if not client.connected:
    print ("Not connected Modbus")
conn = odbc.connect(connection_string)
cursor = conn.cursor()
cursor.execute("select max(id) from Temperature;") # check latest ID in sql base to set new unique id
result = cursor.fetchone()    
Last_Max_ID_SQL = re.sub(r"\D", "", str(result))
print (Last_Max_ID_SQL, " number of last saved record")
if len (Last_Max_ID_SQL) == 0: Last_Max_ID_SQL = 0 # if base empty id = 0
ID_Value = int(Last_Max_ID_SQL) + 1
def is_set(x, n):
    return (x & 2 ** n != 0) 
    # a more bitwise- and performance-friendly version:
    return x & 1 << n != 0
while True:
    PV = client.read_holding_registers(0x4a,1,0x01) #PV value
    SV = client.read_holding_registers(0x4b,1,0x01) #SV value
    SP1 = client.read_holding_registers(0x00,1,0x01) #SP1 given value
    HIAL = client.read_holding_registers(0x01,1,0x01) # HIAL upper limit alarm
    LOAL = client.read_holding_registers(0x02,1,0x01) # LOAL upper limit alarm
    DIAG = client.read_holding_registers(0x4d,1,0x01) # DIAG register DIAG 4d byte  ---> 1 bit -! OP1 (turned on heating)
                                                                     #              ---> 2 bit -! OP2,
                                                                     #              ---> 3 bit -! AU1 - error bit
                                                                     #              ---> 4 bit -! AU2 - error bit,
                                                                     #              ---> 5 bit -! MIO
                                                                     # 0b11111000000000 - state of controller word 4d, without any alarms etc. 
    diag_register = int(DIAG.registers[0])
    OP1 = is_set(diag_register,8)
    OP2 = is_set(diag_register,9)
    AU1 = is_set(diag_register,10)
    AU2 = is_set(diag_register,11)
    MIO = is_set(diag_register,12)
    a = (PV.registers[0]/10)
    b = (SV.registers[0]/10)
    c = (HIAL.registers[0]/10)
    d = (SP1.registers[0]/10)
    e = (LOAL.registers[0]/10)
    time_now = dt.now()
    year = time_now.year
    month = time_now.month
    day=time_now.day
    hour=time_now.hour
    minute=time_now.minute
    seconds=time_now.second
    Date_full1 = time_now.strftime("%d-%m-%Y %H:%M:%S")    
    formatter_string = "%d-%m-%Y %H:%M:%S" 
    datetime_object = datetime.strptime(Date_full1, formatter_string)
    Date_full = datetime_object.replace(year,month,day,hour,minute,seconds)
    cursor.execute("insert into Temperature(ID,PV,SV,HIAL,SP1,LOAL,OP1,OP2,AU1,AU2,MIO,Czas1) values (?,?,?,?,?,?,?,?,?,?,?,?)",(ID_Value,a,b,c,d,e,OP1,OP2,AU1,AU2,MIO,Date_full))
    cursor.commit()
    ID_Value = ID_Value + 1
    time11.sleep(5)
