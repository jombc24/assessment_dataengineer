import requests                              #libreria para usar metodos de HTTP
import os.path                               #libreria para obtener información del sistema
import json                                  #libreria para leer y escribir archivos JSON
from calendar import monthrange              #libreria para extraer datos de fechas

import pandas as pd                          #libreria para modelar los datos obtenidos

#Función para guardar los datos extraidos en archivos JSON
def StoreRawData(RAWData, FileName):
    
    fpath = os.getcwd() + '\json\\'+ FileName['date']+'.json'
    if not os.path.isfile(fpath):            #Crea nuevo archivo
        with open(fpath,'x') as file:
            json.dump(RAWData,file)
    else:                                    #Sobreescribe archivo
        with open(fpath,'w',encoding='utf-8') as file:
            json.dump(RAWData,file)

#Función para cargar los datos desde el API 
def LoadData(source,args):
    
    response = requests.get(source,params=args)
    if response.status_code==200:
        showList=[]
        response_json = response.json()
        n = len(response_json)
        for show in response_json:
            showList.append(show)        
        StoreRawData(showList,args)


if __name__== '__main__':
    url = 'https://api.tvmaze.com/schedule/web'
    num_days = monthrange(2024, 1)[1] # variable para almacenar el total de dias del mes
    for totaldias in range (1, num_days+1):
        checkday = ""
        if totaldias < 10:
             checkday= "0"+str(totaldias)
        else:
            checkday=str(totaldias)
        
        args = { 'date' : '2024-01-'+checkday} 
        LoadData(url,args)
   
        
    #recs =0

    #with open(os.getcwd() + '\json\\'+ args['date']+'.json','r',encoding='utf-8') as file:
    #    recs = json.load(file)
    #print(len(recs))