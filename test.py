print('hello world')
import numpy as np
import pyodbc
import csv

cert = []
raw_cert = []   # certificate number
desc_17 = []    # DESCRIPTION17 data, set points inclusive
set_points = []  # set point data only
desc_20 = []    # DESCRIPTION20 data, mean errors inclusive
mean_errors = []    # mean error
indy_data = []  # event data retrieved from indysoft
qty = []    # quantity of each coil used
coils = []   # number of coils used
raw_event_num = []
event_num = []
dict_desc_20 = {}
dict_indy_data = {}

# csv file reading
with open('bt_data.csv', 'r') as bt_file:
    read = csv.reader(bt_file)
    headers = next(read)    # reading bartender form data headers
    bt_data = next(read)    # reading bartender form data line

    for i in range(0, 10, 2):
        if int(bt_data[i])==0:
            continue
        else:
            qty.append(int(bt_data[i]))

    for i in range(1, 11, 2):
        if bt_data[i]=="":
            continue
        else:
            coils.append(str(bt_data[i]))

    first_sr_no = int(bt_data[11])
    batch = bt_data[12][9:]
    prod_code = bt_data[13][14:]

# indysoft database connection
connection = pyodbc.connect("DRIVER={ODBC Driver 17 for SQL Server};SERVER={server address};UID={username};pwd={password};DATABASE=Indysoft;")
cursor = connection.cursor()

for i,coil in enumerate(coils):
    indy_data.clear()
    desc_20.clear()
    event_num_obj = cursor.execute(f"SELECT EXCELPNTS.EVENT_NUM FROM Indysoft.dbo.excelpnts WHERE EXCELPNTS.DESCRIPTION14 = '{coil}'")
    for row in event_num_obj:
        raw_event_num.append(row)

    cert_num_obj = cursor.execute(f"SELECT CERTS.CERT_NUM FROM CERTS WHERE CERTS.EVENT_NUM = '{int(raw_event_num[i][0])}'")
    for row in cert_num_obj:
        raw_cert.append(row)

    cal_data_obj = cursor.execute(f"SELECT EXCELPNTS.DESCRIPTION17, EXCELPNTS.DESCRIPTION20 FROM EXCELPNTS WHERE EXCELPNTS.EVENT_NUM = '{int(raw_event_num[i][0])}'")
    for row in cal_data_obj:
        indy_data.append(row)
        desc_17.append(row[0])
        desc_20.append(row[1])

    dict_indy_data.update(i:indy_data)
    dict_desc_20.update(i:desc_20)


cursor.close()
connection.close()

# data filteration
# extrating certificate number from repeating list
# for i in len(raw_cert):
#     cert.append(raw_cert[i])

for i in desc_17:
    if i.isalpha():
        tc_type = i

extrating set point data only from DESCRIPTION17
for i in desc_17:
    if i=="":
        del set_points[-3:]
        break
    else:
        set_points.append(i)

# extrating mean errors from DESCRIPTION20
# for i in desc_20:
#     if i=="":
#         break
#     else:
#         mean_errors.append(i)

# extracting event numbers from raw_event_num
for i in raw_event_num:
	event_num.append(i[0])


# total TC quantity
total_qty = sum(qty)
# number of coils used
total_coils = len(list(filter(None, coils)))
# last serial number
last_sr_no = (total_qty+first_sr_no-1)

# delete Later========
print('====================')
print('indysoft data: ', indy_data)
print('total_qty: ',total_qty)
print('quantity for each coils: ',qty)
print('coils used: ',coils)
print('total_coils: ',total_coils)
print('first_sr_no: ',first_sr_no)
print('last_sr_no: ',last_sr_no)
print('batch number: ',batch)
print('product_code: ',prod_code)
print('====================')
# print('TC Type: ',tc_type)
print('event_number: ',event_num)
print('certificate number: ',raw_cert)
print('Set Points are: ',set_points)
print('Mean Errors are: ',mean_errors)
print('=====desc17 and desc20===============')
print(desc_17)
print(desc_20)
print('========dict_desc_20 dict_indy_data===============')
print(dict_indy_data)
print(dict_desc_20)
#  Data Strings for each coils

# write filtered data into a csv file
header = ('Serial_num','Prod_Code','Coil_Num','Cert_Num','Type', 'S_P_0','S_P_1','S_P_2', 'S_P_3', 'S_P_4', 'S_P_5', 'S_P_6', 'S_P_7', 'S_P_8', 'S_P_9', 'S_P_10', 'S_P_11', 'S_P_12', 'S_P_13', 'S_P_14', 'S_P_15', 'S_P_16', 'M_E_0', 'M_E_1', 'M_E_2', 'M_E_3', 'M_E_4', 'M_E_5', 'M_E_6', 'M_E_7', 'M_E_8', 'M_E_9', 'M_E_10', 'M_E_11', 'M_E_12', 'M_E_13', 'M_E_14', 'M_E_15', 'M_E_16')

with open('db_data.csv','w+') as db_data:
	write = csv.writer(db_data)
	write.writerow(header)
