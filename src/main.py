import requests                              #libreria para usar metodos de HTTP
import os.path                               #libreria para obtener información del sistema
import json                                  #libreria para leer y escribir archivos JSON
from calendar import monthrange              #libreria para extraer datos de fechas

import pandas as pd                          #libreria para modelar los datos obtenidos
from ydata_profiling import ProfileReport

def JsonaDataFrame(mes, año,num_days):
    #Función para convertir los archivos JSON del mes en un único DataFrame manteniendo la integridad de los datos
    
    all_data = []                           # Lista para almacenar todos los datos
    
    # Iterar sobre cada día del mes
    for dia in range(1, num_days + 1):
        # Formatear la fecha
        date = f"{año}-{mes:02d}-{dia:02d}"
        file_path = os.path.join(os.getcwd(), 'json', f'{date}.json')
        
        # Verificar si existe el archivo
        if os.path.isfile(file_path):
            with open(file_path, 'r', encoding='utf-8') as file:
                daily_data = json.load(file)
                
                # Agregar la fecha a cada registro
                for record in daily_data:
                    episode_info = {
                        # Información del episodio
                        'id': record['id'],
                        'url': record['url'],
                        'name': record['name'],
                        'season': record['season'],
                        'number': record['number'],
                        'type': record['type'],
                        'airdate': record['airdate'],
                        'airtime': record['airtime'],
                        'airstamp': record['airstamp'],
                        'runtime': record['runtime'],
                        'rating': record['rating'],
                        'image': record['image'],
                        'summary': record['summary'],
                        
                        # Enlaces del episodio
                        'links_self': record['_links']['self']['href'],
                        'links_show_href': record['_links']['show']['href'],
                        'links_show_name': record['_links']['show']['name'],
                        
                        # Información del show embebida
                        'show_id': record['_embedded']['show']['id'],
                        'show_url': record['_embedded']['show']['url'],
                        'show_name': record['_embedded']['show']['name'],
                        'show_type': record['_embedded']['show']['type'],
                        'show_language': record['_embedded']['show']['language'],
                        'show_genres': record['_embedded']['show']['genres'],
                        'show_status': record['_embedded']['show']['status'],
                        'show_runtime': record['_embedded']['show']['runtime'],
                        'show_averageRuntime': record['_embedded']['show']['averageRuntime'],
                        'show_premiered': record['_embedded']['show']['premiered'],
                        'show_ended': record['_embedded']['show']['ended'],
                        'show_officialSite': record['_embedded']['show']['officialSite'],
                        'show_schedule': record['_embedded']['show']['schedule'],
                        'show_rating': record['_embedded']['show']['rating'],
                        'show_weight': record['_embedded']['show']['weight'],
                        'show_network': record['_embedded']['show']['network'],
                        'show_webChannel': record['_embedded']['show']['webChannel'],
                        'show_dvdCountry': record['_embedded']['show']['dvdCountry'],
                        'show_externals': record['_embedded']['show']['externals'],
                        'show_image': record['_embedded']['show']['image'],
                        'show_summary': record['_embedded']['show']['summary'],
                        'show_updated': record['_embedded']['show']['updated'],
                        'show_links': record['_embedded']['show']['_links'],
                        
                        # Agregar la fecha de extracción
                        'extraction_date': date
                    }
                    all_data.append(episode_info)
    
    # Crear DataFrame manteniendo las estructuras anidadas
    df = pd.DataFrame(all_data)
    
    return df

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
       # StoreRawData(showList,args)


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
   
    # Crear DataFrame
    df = JsonaDataFrame(1, 2024,num_days)
    
    # Consultas:
    # Verificar la estructura
    x= [12,4]
    #print(df.columns)  # Ver todas las columnas
    #print(df.iloc[x])  # Ver el primer registro completo
    # Consultar genero del x show 
    #print(df.iloc[x]['show_genres'])
    # País del webchannel del  show
    #print(df.iloc[x]['show_webChannel']['country'])
    
    #Profiling Report
    profile = ProfileReport(df, title="Pandas Profiling Report")
    profile.to_file("./profiling/reporte_profiling.html")
    
    #recs =0

    #with open(os.getcwd() + '\json\\'+ args['date']+'.json','r',encoding='utf-8') as file:
    #    recs = json.load(file)
    #print(len(recs))




    