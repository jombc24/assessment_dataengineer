import requests                              #libreria para usar metodos de HTTP
import os.path                               #libreria para obtener información del sistema
import json                                  #libreria para leer y escribir archivos JSON
from calendar import monthrange              #libreria para extraer datos de fechas

import pandas as pd                          #libreria para modelar los datos obtenidos
from ydata_profiling import ProfileReport
import sqlite3 as sq

def escapec(esc):
    esc = esc.replace('\'', '')
    return esc 

def CustomInsert(sqc, Records, QType):
    it=1
    if QType=="Schema":
        for Query in Records:
            sqc.execute(Query)  
    elif QType=="Genres":
        for record in Records:
            sqc.execute("INSERT INTO Genres (ID, Genre_number) VALUES("+str(it)+", '"+record+"');")
            it+=1
    elif QType=="Country":
        for record in Records:
            Query = "INSERT INTO Countries (Country_ID, Country_name, Country_code, Timezone, Official_site) VALUES("+str(it)+",'"+ record[0]+"', '"+record[1]+"', '"+record[2]+"', '"
            if record[3]is not None:
                Query+=record[3]
            else:
                Query+="N/A"
            Query+="');"
            sqc.execute(Query)
            it+=1
    elif QType=="Types":
        for record in Records:
            sqc.execute("INSERT INTO Show_type (ID, Type) VALUES("+str(it)+", '"+record+"');")
            it+=1
    elif QType=="WChn":
        for record in Records:
            Query = "INSERT INTO web_channels (ID, Channel_name, Country_ID) VALUES("+str(it)+", '"+record[0]+"', "+str(record[3])+");"
            sqc.execute(Query)
            it+=1
    elif QType=="Shows":
        for record in Records:
            SQry = "SELECT ID FROM Show_type WHERE \"Type\" = '"+str(record[4])+"';"
            st = CustomQuery(sqc,SQry, "Get1")   
            Query ="INSERT INTO Show (ID, URL, Name, Season, Chapter_number, Type, Airdate, Airtime, Airstamp, Runtime, Rating, Summary, Embedded, Lenguage, Status, AVGRuntime, Premiered, Ended, Official_site, Weight) VALUES("+str(it)+", '"+str(record[0])+"',"
            Query+="'"+escapec(str(record[1]))+"'," 
            Query+=str(record[2])+","
            Query+="0" 
            Query+=", "+str(st)+", '"+str(record[5])+"', '"+str(record[6])+"', '"+str(record[7])+"', "
            Query += str(record[8])
            Query+=", 'ojo aqui str(record[9])', "
            Query+="'"
            Query+="', 'aqui tambien str(record[11])', '"+str(record[12])+"', '"+str(record[13])+"', "
            Query+= "0" 
            Query+=", '"+str(record[15])+"', '"+str(record[16])+"', '"+str(record[17])+"', "+str(record[18])+");"
            sqc.execute(Query)            
            it+=1  
    elif QType=="ShowGen":
        Query ="SELECT COUNT(*) FROM Show_genres"
        vgen = CustomQuery (sqc,Query,"Get1")
        vgen+=1 
        Query ="INSERT INTO Show_genres (ID, ID_Genre, ID_Show) VALUES("+str(vgen)+", "+str(Records[1])+", "+str(Records[0])+");"
        sqc.execute(Query)            
                 
               
def CustomQuery(sqc, Query, Qtype):
    r = sqc.execute(Query)
    if Qtype=="Get1":
        r=r.fetchone()
        return r[0]
    return

def CreateDB(name,df_parquet):
    print(name)
    try:
        with sq.connect('./db/sec'+name+'.db') as conn:
            
            #Variables Generales
            HArray=[]
            SControler=True
            HArray = [ 
                "CREATE TABLE Genres (ID INTEGER NOT NULL, Genre_number TEXT, CONSTRAINT Genres_PK PRIMARY KEY (ID));",
                "CREATE TABLE Links (ID INTEGER NOT NULL,Show INTEGER,\"Type\" TEXT,URL TEXT,CONSTRAINT Links_PK PRIMARY KEY (ID),CONSTRAINT Links_Show_FK FOREIGN KEY (Show) REFERENCES Show(ID));",
                "CREATE TABLE Countries (Country_ID INTEGER NOT NULL,Country_name TEXT(255),Country_code TEXT(255),Timezone TEXT(255),Official_site TEXT(255),CONSTRAINT Countries_PK PRIMARY KEY (Country_ID));",
                "CREATE TABLE Show_type (ID INTEGER NOT NULL,\"Type\" TEXT(255),CONSTRAINT Show_type_PK PRIMARY KEY (ID));",
                "CREATE TABLE web_channels (ID INTEGER NOT NULL,Channel_name TEXT,Country_ID INTEGER,CONSTRAINT web_channels_pk PRIMARY KEY (ID),CONSTRAINT web_channels_Countries_FK FOREIGN KEY (Country_ID) REFERENCES Countries(Country_ID));",
                "CREATE TABLE Show (ID INTEGER NOT NULL,URL TEXT,Name TEXT,Season INTEGER,Chapter_number INTEGER,\"Type\" INTEGER NOT NULL,Airdate TEXT,Airtime TEXT,Airstamp TEXT,Runtime NUMERIC,Rating TEXT,Summary TEXT,Embedded INTEGER,Lenguage TEXT,Status TEXT,AVGRuntime INTEGER,Premiered TEXT,Ended TEXT,Official_site TEXT,Weight INTEGER,CONSTRAINT Show_PK PRIMARY KEY (ID),CONSTRAINT Show_web_channels_FK FOREIGN KEY (ID) REFERENCES web_channels(ID),CONSTRAINT Show_Show_type_FK FOREIGN KEY (\"Type\") REFERENCES Show_type(ID));",
                "CREATE TABLE Show_genres (ID INTEGER NOT NULL,ID_Genre INTEGER,ID_Show INTEGER,CONSTRAINT Show_genres_PK PRIMARY KEY (ID),CONSTRAINT Show_genres_Genres_FK FOREIGN KEY (ID_Genre) REFERENCES Genres(ID),CONSTRAINT Show_genres_Show_FK FOREIGN KEY (ID_Show) REFERENCES Show(ID));"
                      ]
            

            print(f"Opened SQLite database with version {sq.sqlite_version} successfully.")
            sqc = conn.cursor()

            #Creacion de la base de datos con el esquema previamente diseñado   
            CustomInsert(sqc,HArray,"Schema")


            #Llenado de la base de datos
            
            #Seccion para filtrado y registro de generos
            HArray=[]
            for SData in df_parquet.index:
                for record in df_parquet['show_genres'][SData]:
                    SControler=True
                    for it in HArray:
                        if it==record:
                            SControler=False
                    if SControler: HArray.append(record)
            CustomInsert(sqc, HArray, "Genres")
            print("Generos registrados: "+str(len(HArray))) #Impresion de control

           
            #Seccion para filtrado y registro de paises
            HArray=[]
            for SData in df_parquet.index:
                for record in df_parquet['show_webChannel'][SData] or []:
                    SArray=df_parquet['show_webChannel'][SData]
                    CountryData=[]
                    Country = SArray["country"]
                    if Country is not None:
                        CountryData.append(Country["name"])
                        CountryData.append(Country["code"])
                        CountryData.append(Country["timezone"])
                    else:
                        CountryData.append("N/A")
                        CountryData.append("N/A")
                        CountryData.append("N/A")
                    CountryData.append(SArray["officialSite"])
                    
                    SControler=True
                    for c in HArray:
                        if c[0]==CountryData[0] and c[1]==CountryData[1]and c[2]==CountryData[2]and c[3]==CountryData[3]:
                            SControler=False
                    if SControler: HArray.append(CountryData)
            CustomInsert(sqc,HArray,"Country")
            print("Paises registrados: "+str(len(HArray))) #Impresion de control


            #Seccion para filtrado y registro de tipos de show
            HArray=[]
            for SData in df_parquet.index:
                for record in df_parquet["type"]:
                    SControler=True
                    for it in HArray:
                        if it==record:
                            SControler=False
                    if SControler: HArray.append(record)
                for record in df_parquet['show_type']:
                    SControler=True
                    for it in HArray:
                        if it==record:
                            SControler=False
                    if SControler: HArray.append(record)
            CustomInsert(sqc,HArray,"Types")
            print("Tipos de Show registrados: "+str(len(HArray))) #Impresion de control 
                    

            #Seccion para filtrado y registro del Canales Web
            HArray=[]
            for SData in df_parquet.index:
                for record in df_parquet['show_webChannel'][SData] or []:
                    SArray=df_parquet['show_webChannel'][SData]
                    CountryData=[]
                    a=[]
                    a.append(SArray["name"])
                    Country = SArray["country"]
                    if Country is not None:
                        CountryData.append(Country["name"])
                        CountryData.append(Country["code"])
                        CountryData.append(Country["timezone"])
                    else:
                        CountryData.append("N/A")
                        CountryData.append("N/A")
                        CountryData.append("N/A")
                    CountryData.append(SArray["officialSite"])
                    a.append(CountryData[0])             
                    a.append(CountryData[3])
                    SControler=True
                    for c in HArray:
                        if c[0]==a[0] and c[1]==a[1]:
                            SControler=False
                    if SControler:
                        SQry="SELECT Country_ID FROM Countries WHERE Country_name = '"
                        SQry+=a[1]
                        SQry+="' AND Official_site = '"
                        if a[2] is not None:
                            SQry+=a[2]
                        else:
                            SQry+="N/A"
                        SQry+="';"                        
                        id = CustomQuery(sqc,SQry, "Get1")                        
                        a.append(id)                        
                        HArray.append(a)
            CustomInsert(sqc,HArray,"WChn")
            print("Canales Web registrados: "+str(len(HArray))) #Impresion de control 


            #Seccion para filtrado y registro de shows
            df_parquet = df_parquet.fillna('NULL')
            HArray=[]
            for SData in df_parquet.index:                  
                infoshow = []
                transfer = ""
                transfer = df_parquet ["url"][SData]                
                infoshow.append(transfer)
                transfer = df_parquet ["name"][SData]               
                infoshow.append(transfer)
                transfer = df_parquet ["season"][SData]
                infoshow.append(transfer)
                transfer = df_parquet ["number"][SData]
                infoshow.append(transfer)
                transfer = df_parquet ["type"][SData]
                infoshow.append(transfer)
                transfer = df_parquet ["airdate"][SData]
                infoshow.append(transfer)
                transfer = df_parquet ["airtime"][SData]
                infoshow.append(transfer)
                transfer = df_parquet ["airstamp"][SData]
                infoshow.append(transfer)
                transfer = df_parquet ["runtime"][SData]
                infoshow.append(transfer)
                transfer = df_parquet ["rating"][SData]
                infoshow.append(transfer)
                transfer = df_parquet ["summary"][SData]
                infoshow.append(transfer)
                transfer = ""             
                infoshow.append(transfer)
                transfer = df_parquet ["show_language"][SData]
                infoshow.append(transfer)                
                transfer = df_parquet ["show_status"][SData]
                infoshow.append(transfer)
                transfer = df_parquet ["show_averageRuntime"][SData]
                infoshow.append(transfer)
                transfer = df_parquet ["show_premiered"][SData]
                infoshow.append(transfer)
                transfer = df_parquet ["show_ended"][SData]
                infoshow.append(transfer)
                transfer = df_parquet ["show_officialSite"][SData]
                infoshow.append(transfer)
                transfer = df_parquet ["show_weight"][SData]
                infoshow.append(transfer)
                HArray.append(infoshow)
            CustomInsert(sqc,HArray,"Shows")
            print("Show registrados: "+str(len(HArray))) #Impresion de control 


            #Seccion para registro de Generos por Show
            HArray=[]
            for SData in df_parquet.index:
                NVal=[]
                for record in df_parquet['show_genres'][SData]:
                   
                    #get show id
                    SQry = "SELECT ID FROM Show WHERE URL ='"+df_parquet ["url"][SData] +"'"
                    shid = CustomQuery(sqc,SQry, "Get1")  
                    NVal.append(shid)
                    #get gen id
                    SQry = "SELECT ID FROM Genres WHERE Genre_number ='"+record +"'" 
                    grid = CustomQuery(sqc,SQry, "Get1") 
                    NVal.append(grid)
                    CustomInsert(sqc,NVal,"ShowGen") 
                                                                     
            print("Generos de Show registrados: "+str(len(HArray))) #Impresion de control
                  
    except sq.OperationalError as e:
        print("Failed to open database:", e)
    
def ExportToSQL(name,db):
    df_parquet = pd.read_parquet('./data/'+name+'.parquet')
    CreateDB(db,df_parquet)      

def SaveParquet(df,name):
    df.to_parquet("./data/"+name+".parquet",compression='snappy')

def DataCleanup(df):
    # Columnas a eliminar (>70% valores nulos)
    columns_to_drop = ['show_dvdCountry', 'show_webChannel', 'show_externals','show_image','show_runtime','image','show_schedule']
    df = df.drop(columns=columns_to_drop)
    # Imputar con la media/mediana
    df['runtime'] = df['runtime'].fillna(df['runtime'].mean())
    df['show_averageRuntime'] = df['show_averageRuntime'].fillna(df['show_averageRuntime'].mean())
    # Imputar con valor más frecuente
    df['show_language'] = df['show_language'].fillna(df['show_language'].mode()[0])
    # Imputar con "Unknown" o valor específico
    df['show_officialSite'] = df['show_officialSite'].fillna("Unknown")
    df['summary'] = df['summary'].fillna("No summary available")
    # Convertir campos JSON a estructuras de datos
    df['show_genres'] = df['show_genres'].apply(lambda x: eval(x) if isinstance(x, str) else x)
    df['show_rating'] = df['show_rating'].apply(lambda x: eval(x) if isinstance(x, str) else x)
    # Normalizar tipos
    df['type'] = df['type'].str.lower()
    df['show_status'] = df['show_status'].str.lower()
    df['show_language'] = df['show_language'].str.lower()
    GenerateProfiling(df, "posterior")

def Consultas(df,vis):
    
    # Consultas:
    # Verificar la estructura
    x= [12,4]
    if vis: print(df.columns)  # Ver todas las columnas
    if vis: print(df.iloc[x])  # Ver el primer registro completo
    # Consultar genero del x show 
    if vis: print(df.iloc[x]['show_genres'])
    # País del webchannel del  show
    if vis: print(df.iloc[x]['show_webChannel'])
    
def GenerateProfiling(df,status):
    #Profiling Report
    profile = ProfileReport(df, title="Pandas Profiling Report")
    profile.to_file("./profiling/reporte_profiling-"+status+".html")
    
    #recs =0

    #with open(os.getcwd() + '\json\\'+ args['date']+'.json','r',encoding='utf-8') as file:
    #    recs = json.load(file)
    #print(len(recs))

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
        StoreRawData(response_json,args)


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
    Consultas(df, False)
    GenerateProfiling(df,"previa")
    SaveParquet(df,name="Compressed_previa")
    DataCleanup(df)
    SaveParquet(df,name="Compressed_posterior")
    ExportToSQL("Compressed_posterior","Tvmaze_shows")


    




    