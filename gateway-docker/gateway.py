import logging
import uvicorn
from fastapi import FastAPI, UploadFile
from pydantic import BaseModel
import uuid
import sqlite3
import threading
from dotenv import load_dotenv
import os
import requests
import json
import sys

load_dotenv()

GatewayDir = os.getenv('GatewayDir')
GatewayURL = os.getenv('GatewayURL')
GatewayPort = int(os.getenv('GatewayPort'))
ConnectorURL = os.getenv('ConnectorURL')
ConnectorPort = os.getenv('ConnectorPort')
ConnectorVerdictUri = os.getenv('ConnectorVerdictUri')
SBHost = os.getenv('SB_host')
SBtoken = os.getenv('SB_token')
SBAPIUpload = os.getenv('SB_ApiUpload')
SBAPIScanTask = os.getenv('SB_ApiScanTask')
AnalysisDepth = os.getenv('SB_analysis_depth')
SBsendFileTimeout = float(os.getenv('SB_sendFileTimeout'))
ConnectorSendVerdictTimeout = float(os.getenv('ConnectorSendVerdictTimeout'))
LoggingLevel = os.getenv('LoggingLevel')

DBFile = f'{GatewayDir}/SBscan.db'

logging.basicConfig(filename=f'{GatewayDir}/gateway.log', level=logging.getLevelName(LoggingLevel), format=' %(asctime)s - %(levelname)s - %(message)s', encoding="utf-8")
app = FastAPI()


connection = sqlite3.connect(DBFile)
cursor = connection.cursor()
createTebleQuery = '''
CREATE TABLE IF NOT EXISTS Files (
uuid TEXT NOT NULL PRIMARY KEY,
name TEXT,
ruid TEXT,
file BLOB NOT NULL,               
stage TEXT NOT NULL,
file_uri TEXT,
verdict TEXT                               
)
'''
cursor.execute(createTebleQuery)
connection.commit()
connection.close()

@app.post("/fileUpload/")
async def scanItem(file: UploadFile):
    try:
        fileuuid = uuid.uuid4()
        logging.debug(f'UploadedFile: {file.filename}, {fileuuid}')
        fileContent = await file.read()
        s_fileuuid = f'{fileuuid}'
        connection = sqlite3.connect(DBFile)
        cursor = connection.cursor()
        cursor.execute('INSERT INTO Files (file, uuid, stage, name) VALUES (?,?,?,?)', (fileContent, s_fileuuid, 'queued', file.filename))
        connection.commit()
        connection.close()
        return {"uuid": fileuuid}
    except Exception as e:
        logging.error(f'File Upload Failed: {repr(e)}')  
        sys.exit(1)

class Item(BaseModel):
    RUID: str
    UUID: str
@app.post("/fileRUID/")
async def setRUID(item:Item):
    try:
        logging.debug(f'RUID Params: {item.UUID}, {item.RUID}')
        connection = sqlite3.connect(DBFile)
        cursor = connection.cursor()
        cursor.execute('UPDATE Files SET ruid = ? WHERE uuid =?', (item.RUID, item.UUID))
        connection.commit()
        connection.close()
        #cursor.execute('SELECT * from Files where uuid=?', (item.UUID,))
        #results = cursor.fetchall()
        #logging.debug(f'SQLie Qeury Select file by UUID: {results}')
        return {"UUID":item.UUID, "RUID": item.RUID}
    except Exception as e:
        logging.error(f'Set RUID Failed: {repr(e)}')  
        sys.exit(1)

if __name__ == '__main__':
    uvicorn.run(app='gateway:app', host=GatewayURL, port=GatewayPort, log_config=None)

def writeTofile(data, filename):
    with open(filename, 'wb') as file:
        file.write(data)

def sendFileToSandbox():
    try:
        threading.Timer(SBsendFileTimeout,sendFileToSandbox).start()
        headers = {'X-API-Key': SBtoken}
        SBFileUploadURL = SBHost + SBAPIUpload
        connection = sqlite3.connect(DBFile)
        cursor = connection.cursor()
        cursor.execute('SELECT name, file, uuid from Files where stage=?', ('queued',))
        results = cursor.fetchall()
        for file in results:
           try:
                logging.debug(f'FileName: {file[0]}')
                filename = file[0]
                fileContent = file[1]
                fileUUID = file[2]
                filePath = f'{GatewayDir}/{filename}'
                writeTofile(fileContent, filePath) 
                files = {'file': open(filePath, "rb")}
                UploadFileResponse = requests.post(SBFileUploadURL, files=files, headers=headers, verify=False)
                logging.debug(f'Upload: {UploadFileResponse.text}')
                if UploadFileResponse.status_code == 200:
                     os.remove(filePath)
                     UploadFileResult = UploadFileResponse.json()
                     logging.debug(f'UploadResult: {UploadFileResult["data"]["file_uri"]}')
                     fileUri = UploadFileResult["data"]["file_uri"]
                     cursor = connection.cursor()
                     cursor.execute('UPDATE Files SET file_uri = ?, stage = ?  WHERE uuid =?', (fileUri,'send',fileUUID))
                     connection.commit()
                     #cursor.execute('SELECT * from Files where uuid=?', (fileUUID,))
                     #results = cursor.fetchall()
                     #logging.debug(f'SQLie Qeury Select file by UUID: {results}')
                     SBFileChekURL = SBHost + SBAPIScanTask
                     payload = {'file_uri': fileUri, 
                                 'file_name': filename,
                                 'options': {
                                     'analysis_depth': AnalysisDepth,
                                     'url_extract_enabled' : True 
                            }
                           }
                     payloadjson = json.dumps(payload)
                     logging.debug(f'PayloadJson: {payloadjson}')
                     checkFileResponse = requests.post(SBFileChekURL, headers= headers, data = payloadjson, verify=False)
                     logging.debug(f'ScanResult: {checkFileResponse.text}')
                     if checkFileResponse.status_code == 200:
                         checkFileResponseResult = checkFileResponse.json() 
                         verdict = checkFileResponseResult["data"]["result"]["verdict"]
                         cursor = connection.cursor()
                         cursor.execute('UPDATE Files SET stage = ?, verdict = ? WHERE uuid =?', ('verdict', verdict,fileUUID))
                         connection.commit()
                         #cursor.execute('SELECT * from Files where uuid=?', (fileUUID,))
                         #results = cursor.fetchall()
                         #logging.debug(f'SQLie Qeury Select file by UUID: {results}')
                         cursor.close()
                     else:
                         cursor.close()
                         raise ValueError(f'Scan return code: {checkFileResponse.status_code}')     
                else:
                    cursor.close()
                    raise ValueError(f'Upload return code: {UploadFileResponse.status_code}')     
           except ValueError as e:
                logging.error(f'Failed UploadToSandBox: {repr(e)}')
                continue
    except Exception as e:
        logging.error(f'Failed sendFiletoSandBox: {repr(e)}')  
        sys.exit(1)

def sendVerdictToConnector():
    try:
        threading.Timer(ConnectorSendVerdictTimeout,sendVerdictToConnector).start()
        connectorAPIURL = f'{ConnectorURL}:{ConnectorPort}{ConnectorVerdictUri}'
        connection = sqlite3.connect(DBFile)
        cursor = connection.cursor()
        cursor.execute('SELECT ruid, uuid, verdict  from Files where stage=?', ('verdict',))
        results = cursor.fetchall()
        if results != []:
            for file in results:
                try:
                    fileRUID = file[0]
                    fileUUID = file[1]
                    fileVerdict = file[2]
                    if fileVerdict == "CLEAN":
                        data = {'RUID' : fileRUID,
                                'ARG' : "OK"
                                }
                    else:
                        data = {'RUID' : fileRUID,
                                'ARG' : "DISCARD"
                                }
                    payload = json.dumps(data)
                    setConnectorResponse = requests.post(connectorAPIURL, data=payload, verify=False)
                    logging.debug(f'SetRUIDResult: {setConnectorResponse.status_code}')
                    if setConnectorResponse.status_code == 200:
                        cursor.close()
                        connection = sqlite3.connect(DBFile)
                        cursor = connection.cursor()
                        cursor.execute('DELETE from Files where uuid=?', (fileUUID,))  
                        connection.commit()
                    else:
                        cursor.close()
                        raise ValueError(f'Upload return code: {setConnectorResponse.status_code}')     
                except ValueError as e:
                    logging.error(f'Failed UploadToGateway: {repr(e)}')
                    continue    
        cursor.close()
    except Exception as e:
       logging.error(f'Failed sendVerdictToConnector: {repr(e)}')  
       sys.exit(1)         
sendFileToSandbox()
sendVerdictToConnector()
       
