# Importaciones
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse
import api_functions as af
import pandas as pd
import os

import importlib
importlib.reload(af)

# Se instancia la aplicación
app = FastAPI()

#Comenzamos a trabajar las instancias de fastAPI
df_items_developer = pd.read_parquet('archivos_parquet/df_items_developer.parquet')
df_generos=pd.read_parquet('archivos_parquet/max_por_gen.parquet')
# Tomo solo un 10% de mi df:
df_generos= df_generos.sample(frac=0.1,random_state=42)

#Funcion 1: (Developer)
@app.get(path = '/developer',
          description = """ <font color="blue">
                        1. Haga clik en "Try it out".<br>
                        2. Ingrese el nombre del desarrollador en el box abajo.<br>
                        3. Scrollear a "Resposes" para ver la cantidad de items y porcentaje de contenido Free por año de ese desarrollador.
                        </font>
                        """,
         tags=["Consultas Generales"])
def developer(desarrollador):
    '''
    Esta función devuelve información sobre una empresa desarrolladora de videojuegos.
         
    Args:
        desarrollador (str): Nombre del desarrollador de videojuegos.
    
    Returns:
        dict: Un diccionario que contiene información sobre la empresa desarrolladora.
            - 'cantidad_por_año' (dict): Cantidad de items desarrollados por año.
            - 'porcentaje_gratis_por_año' (dict): Porcentaje de contenido gratuito por año según la empresa desarrolladora.
    '''
    # Filtra el dataframe por desarrollador de interés
    data_filtrada = df_items_developer[df_items_developer['developer'] == desarrollador]
    # Calcula la cantidad de items por año
    cantidad_por_año = data_filtrada.groupby('año_lanzamiento')['item_id'].count()
    # Calcula la cantidad de elementos gratis por año
    cantidad_gratis_por_año = data_filtrada[data_filtrada['price'] == 0.0].groupby('año_lanzamiento')['item_id'].count()
    # Calcula el porcentaje de elementos gratis por año
    porcentaje_gratis_por_año = (cantidad_gratis_por_año / cantidad_por_año * 100).fillna(0).astype(int)

    result_dict = {
        'cantidad_items_por_año': cantidad_por_año.to_dict(),
        'porcentaje_gratis_por_año': porcentaje_gratis_por_año.to_dict()
    }
    
    return result_dict


#Funcion 2: (userdata)
@app.get(path = '/userdata',
          description = """ <font color="blue">
                        INSTRUCCIONES<br>
                        1. Haga clik en "Try it out".<br>
                        2. Ingrese el user_id en el box abajo.<br>
                        3. Scrollear a "Resposes" para ver la cantidad de dinero gastado por el usuario, el porcentaje de recomendación que realiza el usuario y cantidad de items que tiene el mismo.
                        </font>
                        """,
         tags=["Consultas Generales"])
def userdata(user_id: str = Query(..., 
                                description="Identificador único del usuario", 
                                examples="EchoXSilence")):
        
    return af.userdata(user_id)

#Funcion 3: (userForGenre)
#Para este caso en particular hice la ejecucion de la funcion dentro de mi app.get ya que tenia problemas para llamar la funcion.
@app.get('/genero', 
            description = """ <font color="blue">
                        1. Haga clik en "Try it out".<br>
                        2. Ingrese el genero que desea consultar en el box abajo.<br>
                        3. Scrollear a "Resposes" para ver el usuario que acumula mas horas jugadas y la lista de horas jugas para el año del lanzamiento.
                        </font>
                        """,
         tags=["Consultas Generales"])
def UserForGenre(genero:str):
    """
    Esta funcion devuelve para un genero dado, el usuario que acumula mas horas desde el lanzamiento del juego, y la cantidad de horas totales en cada año
    params:
    genero:str Genero de un juego
    """
    try:
        if genero.lower() not in [x.lower() for x in df_generos['Género'].tolist()]:
            return "No se encontró ese genero"
        
        gen = df_generos[df_generos['Género'].str.lower() == genero.lower()] # Busco el genero especificado
        
        return { 
            'Usuario':gen['Usuario'].tolist(),
            'Horas jugadas':gen['Año_Horas'].tolist()
        }
    except Exception as e:
        return {"Error":str(e)}
    
#Funcion 4: (best_developer_year)
@app.get('/best_developer_year/{year}',
         description = """ <font color="blue">
                        1. Haga clik en "Try it out".<br>
                        2. Ingrese el año que desea consultar en el box abajo.<br>
                        3. Scrollear a "Resposes" para ver el top 3 de desarrolladores con juegos mas recomendados.
                        </font>
                        """,
         tags=["Consultas Generales"])   
def best_developer_year(year:int):
    """
    Esta funcion calcula para un año dado, el top de los  tres desarrolladores con mas juegos.
    params:
    year:int : Año
    """
    try:  
        # Carga los datos de los juegos de steam
        df_games = pd.read_csv('archivos csv/steam_games_limpio.csv')
        # Tomo solo un 10% de mi df:
        df_games= df_games.sample(frac=0.1,random_state=42)
        # Carga las revisiones de los usuarios
        df_reviews = pd.read_csv('archivos csv/user_reviews_limpio.csv')
        # Tomo solo un 10% de mi df:
        df_reviews= df_reviews.sample(frac=0.1,random_state=42)
        # Elimino columnas que nos seran necesarias en el estudio
        df_games=df_games.drop(['publisher','title','early_access'],axis=1)
        # Elimina las filas con valores faltantes en los datos de los juegos
        df_games.dropna(inplace =True)
        # Convierte el año de lanzamiento,user_id,id a int
        df_games['año_lanzamiento'] = df_games['año_lanzamiento'].astype(int)
        df_games['id'] = df_games['id'].astype(int)
        df_reviews['reviews_item_id'] = df_reviews['reviews_item_id'].astype(int)
        # Une los datos de los juegos y las revisiones en 'id'
        func_4 = pd.merge(df_reviews,df_games,left_on='reviews_item_id',right_on='id',how='inner')
        # Filtra los datos para obtener solo los juegos lanzados en el año dado
        func_4 = func_4[func_4['año_lanzamiento'] == year]
        # Agrupa los datos por desarrollador 
        mejores_dev = func_4.groupby('developer')['reviews_recommend'].sum().reset_index().sort_values(by='reviews_recommend',ascending=False)
        # Verifica si no se encontraron desarrolladores con revisiones en ese año.
        if mejores_dev.empty:
            return 'No se encontraron reviews para items que hayan salido ese año'
        else:
            # Obtiene los tres primeros desarrolladores con más recomendaciones
            puesto1 = mejores_dev.iloc[0][0]
            puesto2 = mejores_dev.iloc[1][0]
            puesto3 = mejores_dev.iloc[2][0]
            puestos = {"Puesto 1": str(puesto1), "Puesto 2":str(puesto2), "Puesto 3": str(puesto3)}
            # Devuelve los tres primeros desarrolladores con más recomendaciones
            return puestos
    except Exception as e:
        return {"Error":str(e)}
#Funcion 5
@app.get('/recommend/{developer_rec}',
         description = """ <font color="blue">
                        1. Haga clik en "Try it out".<br>
                        2. Ingrese el desarrollador que desea consultar en el box abajo.<br>
                        3. Scrollear a "Resposes" para ver el registro de reseñas positivas y negativas.
                        </font>
                        """,
         tags=["Consultas Generales"]) 
def developer_rec(developer_rec:str):
    """
    Esta funcion calcula para un desarrolador la cantidad de usuarios con reviews positivas y negativas.
    params:
    developer_rec:str : Desarrolador
    
    
    """
    try:
        # Carga los datos de los juegos de steam
        df_games = pd.read_csv('archivos csv/steam_games_limpio.csv')
        # Tomo solo un 10% de mi df:
        df_games= df_games.sample(frac=0.1,random_state=42)
        # Carga las revisiones de los usuarios
        df_reviews = pd.read_csv('archivos csv/df_reviews.csv')
        # Tomo solo un 10% de mi df:
        df_reviews= df_reviews.sample(frac=0.1,random_state=42)
        df_games['id'] = df_games['id'].astype(int)
        df_reviews['reviews_item_id'] = df_reviews['reviews_item_id'].astype(int)
        # Merging los dos datasets, con una combinación interna en sus respectivos 'id'
        func_5 = pd.merge(df_reviews,df_games,left_on='reviews_item_id',right_on='id',how='inner')
        # Convertir todos los nombres de los desarrolladores en letras minúsculas para evitar la duplicación de datos debido a las diferencias de mayúsculas y minúsculas
        func_5['developer'] = func_5['developer'].str.lower()

        # Convertir el nombre del desarrollador proporcionado en letras minúsculas
        developer_rec2 = developer_rec.lower()
        # Filtrar por desarrollador
        func_5 = func_5[func_5['developer'] == developer_rec2]
        # Verificar si se encuentra los juegos del desarrollador en el dataset
        if func_5.empty:
            # En caso de que no se encuentre, se muestra mensaje indicando que no hay comentarios para este desarrollador
            return 'No se encontraron reviews para ese desarrollador'
        # En caso contrario, contar los sentimientos de análisis de comentarios
        # Cuenta los comentarios positivos
        true_value = func_5[func_5['sentiment_analysis']==2]['sentiment_analysis'].count()
        # Cuenta los comentarios negativos
        false_value = func_5[func_5['sentiment_analysis']==0]['sentiment_analysis'].count()
        # Devolver conteos en un diccionario
        return {developer_rec2:[f'Negative = {int(false_value)}',f'Positive = {int(true_value)}']}
    except Exception as e:
        return {"Error":str(e)}
#Funcion 6
@app.get('/recomendacion_juego',
         description=""" <font color="blue">
                    INSTRUCCIONES<br>
                    1. Haga clik en "Try it out".<br>
                    2. Ingrese el nombre de un juego en box abajo.<br>
                    3. Scrollear a "Resposes" para ver los juegos recomendados.
                    </font>
                    """,
         tags=["Recomendación"])
def recomendacion_juego(game: str = Query(..., 
                                         description="Juego a partir del cuál se hace la recomendación de otros juego", 
                                         examples="Killing Floor")):
    return af.recomendacion_juego(game)
#Funcion 7
@app.get('/recomendacion_usuario',
         description=""" <font color="blue">
                    INSTRUCCIONES<br>
                    1. Haga clik en "Try it out".<br>
                    2. Ingrese el id del usuario en box abajo.<br>
                    3. Scrollear a "Resposes" para ver los juegos recomendados para ese usuario.
                    </font>
                    """,
         tags=["Recomendación"])
def recomendacion_usuario(user: str = Query(..., 
                                         description="Usuario a partir del cuál se hace la recomendación de los juego", 
                                         examples="76561197970982479")):
    return af.recomendacion_usuario(user)