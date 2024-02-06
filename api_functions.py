## FUNCIONES A UTILIZAR EN app.py

# Importaciones
import pandas as pd
import operator

# Datos a usar


df_reviews = pd.read_parquet('archivos_parquet/df_reviews.parquet')
df_gastos_items = pd.read_parquet('archivos_parquet/df_gastos_items.parquet')
piv_norm = pd.read_parquet('archivos_parquet/piv_norm.parquet')
item_sim_df = pd.read_parquet('archivos_parquet/item_sim_df.parquet')
user_sim_df = pd.read_parquet('archivos_parquet/user_sim_df.parquet')


def userdata(user_id):
    '''
    Esta función devuelve información sobre un usuario según su 'user_id'.
         
    Args:
        user_id (str): Identificador único del usuario.
    
    Returns:
        dict: Un diccionario que contiene información sobre el usuario.
            - 'cantidad_dinero' (int): Cantidad de dinero gastado por el usuario.
            - 'porcentaje_recomendacion' (float): Porcentaje de recomendaciones realizadas por el usuario.
            - 'total_items' (int): Cantidad de items que tiene el usuario.
    '''
    # Filtra por el usuario de interés
    usuario = df_reviews[df_reviews['user_id'] == user_id]
    # Calcula la cantidad de dinero gastado para el usuario de interés
    cantidad_dinero = df_gastos_items[df_gastos_items['user_id']== user_id]['price'].iloc[0]
    # Busca el count_item para el usuario de interés    
    count_items = df_gastos_items[df_gastos_items['user_id']== user_id]['items_count'].iloc[0]
    
    # Calcula el total de recomendaciones realizadas por el usuario de interés
    total_recomendaciones = usuario['reviews_recommend'].sum()
    # Calcula el total de reviews realizada por todos los usuarios
    total_reviews = len(df_reviews['user_id'].unique())
    # Calcula el porcentaje de recomendaciones realizadas por el usuario de interés
    porcentaje_recomendaciones = (total_recomendaciones / total_reviews) * 100
    
    return {
        'cantidad_dinero': int(cantidad_dinero),
        'porcentaje_recomendacion': round(float(porcentaje_recomendaciones), 2),
        'total_items': int(count_items)
    }

def best_developer_year_func(year:int):
    # Carga los datos de los juegos de steam
    df_games = pd.read_csv('archivos csv/df_games.csv')
    # Tomo solo un 10% de mi df:
    df_games= df_games.sample(frac=0.1,random_state=42)
    # Carga las revisiones de los usuarios
    df_reviews = pd.read_csv('archivos csv/df_reviews.csv')
    # Tomo solo un 10% de mi df:
    df_reviews= df_reviews.sample(frac=0.1,random_state=42)
    # Elimino columnas que nos seran necesarias en el estudio
    df_games=df_games.drop(['publisher','title','early_access'],axis=1)
    # Elimina las filas con valores faltantes en los datos de los juegos
    df_games.dropna(inplace =True)
    # Convierte el año de lanzamiento a int
    df_games['año_lanzamiento'] = df_games['año_lanzamiento'].astype(int)
    # Une los datos de los juegos y las revisiones en 'id'
    func_4 = pd.merge(df_reviews,df_games,left_on='item_id',right_on='id',how='inner')
    # Filtra los datos para obtener solo los juegos lanzados en el año dado
    func_4 = func_4[func_4['año_lanzamiento'] ==year]
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

def recomendacion_juego(game):

    '''
    Muestra una lista de juegos similares a un juego dado.

    Args:
        game (str): El nombre del juego para el cual se desean encontrar juegos similares.

    Returns:
        None: Un diccionario con 5 nombres de juegos recomendados.

    '''
    # Obtener la lista de juegos similares ordenados
    similar_games = item_sim_df.sort_values(by=game, ascending=False).iloc[1:6]

    count = 1
    contador = 1
    recomendaciones = {}
    
    for item in similar_games:
        if contador <= 5:
            item = str(item)
            recomendaciones[count] = item
            count += 1
            contador += 1 
        else:
            break
    return recomendaciones

def recomendacion_usuario(user):
    '''
    Genera una lista de los juegos más recomendados para un usuario, basándose en las calificaciones de usuarios similares.

    Args:
        user (str): El nombre o identificador del usuario para el cual se desean generar recomendaciones.

    Returns:
        list: Una lista de los juegos más recomendados para el usuario basado en la calificación de usuarios similares.

    '''
    # Verifica si el usuario está presente en las columnas de piv_norm (si no está, devuelve un mensaje)
    if user not in piv_norm.columns:
        return('No data available on user {}'.format(user))
    
    # Obtiene los usuarios más similares al usuario dado
    sim_users = user_sim_df.sort_values(by=user, ascending=False).index[1:11]
    
    best = [] # Lista para almacenar los juegos mejor calificados por usuarios similares
    most_common = {} # Diccionario para contar cuántas veces se recomienda cada juego
    
    # Para cada usuario similar, encuentra el juego mejor calificado y lo agrega a la lista 'best'
    for i in sim_users:
        i = str(i)
        max_score = piv_norm.loc[:, i].max()
        best.append(piv_norm[piv_norm.loc[:, i]==max_score].index.tolist())
    
    # Cuenta cuántas veces se recomienda cada juego
    for i in range(len(best)):
        for j in best[i]:
            if j in most_common:
                most_common[j] += 1
            else:
                most_common[j] = 1
    
    # Ordena los juegos por la frecuencia de recomendación en orden descendente
    sorted_list = sorted(most_common.items(), key=operator.itemgetter(1), reverse=True)
    recomendaciones = {} 
    contador = 1 
    # Devuelve los 5 juegos más recomendados
    for juego, _ in sorted_list:
        if contador <= 5:
            recomendaciones[contador] = juego 
            contador += 1 
        else:
            break
    
    return recomendaciones