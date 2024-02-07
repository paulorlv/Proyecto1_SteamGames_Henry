![](https://github.com/paulorlv/Proyecto1_SteamGames_Henry/blob/main/IMG-20240206-WA0040.jpg)
# Proyecto de MLOps de Steam

### Esquema de contenido:
- [Introduccion](http://https://github.com/paulorlv/Proyecto1_SteamGames_Henry#Introduccion)
- [Contexto](http://https://github.com/paulorlv/Proyecto1_SteamGames_Henry/Contexto)
- [Desarrollo](http://https://github.com/paulorlv/Proyecto1_SteamGames_Henry/Desarrollo)
 - [ETL](http://https://github.com/paulorlv/Proyecto1_SteamGames_Henry/ETL)
 - [Feature engineering](http://https://github.com/paulorlv/Proyecto1_SteamGames_Henry/Featureengineering)
  - [EDA](http://https://github.com/paulorlv/Proyecto1_SteamGames_Henry/EDA)
  - [Modelo de aprendizaje automático](http://https://github.com/paulorlv/Proyecto1_SteamGames_Henry/Modelodeaprendizajeautomático)
  - [Desarrollo de API](http://https://github.com/paulorlv/Proyecto1_SteamGames_Henry/DesarrollodeAPI)
  - [Deployment](http://https://github.com/paulorlv/Proyecto1_SteamGames_Henry/Deployment)
  - [Video](http://https://github.com/paulorlv/Proyecto1_SteamGames_Henry/Video)

### Introduccion
Este proyecto simula el rol de un MLOps Engineer, es decir, la combinación de un Data Engineer y Data Scientist, para la plataforma multinacional de videojuegos Steam. Para su desarrollo, se entregan unos datos y se solicita un Producto Mínimo Viable que muestre una API deployada en un servicio en la nube y la aplicación de dos modelos de Machine Learning, por una lado, un análisis de sentimientos sobre los comentarios de los usuarios de los juegos y, por otro lado, la recomendación de juegos a partir de dar el nombre de un juego y/o a partir de los gustos de un usuario en particular.

### Contexto

Steam es una plataforma de distribución digital de videojuegos desarrollada por Valve Corporation. Fue lanzada en septiembre de 2003 como una forma para Valve de proveer actualizaciones automáticas a sus juegos, pero finalmente se amplió para incluir juegos de terceros. Cuenta con más de 325 millones de usuarios y más de 25.000 juegos en su catálogo. Es importante tener en cuenta que las cifras publicadas por SteamSpy son hasta el año 2017, porque a principios de 2018 Steam limitó la forma de obtener estadísticas, por eso no hay datos tan precisos.

### Desarrollo
#### ETL
Se realizó la extracción, transformación y carga (ETL) de los tres conjuntos de datos entregados. Dos de los conjuntos de datos se encontraban anidados, es decir había columnas con diccionarios o listas de diccionarios, por lo que aplicaron distintas estrategias para transformar las claves de esos diccionarios en columnas. Luego se rellenaron algunos nulos de variables necesarias para el proyecto, se borraron columnas con muchos nulos o que no aportaban al proyecto, para optimizar el rendimiento de la API y teneniendo en cuenta las limitaciones de almacenamiento del deploy. Para las transformaciones se utilizó la librería Pandas.

Los detalles del ETL se puede ver en [ETL_steam_games](https://github.com/paulorlv/Proyecto1_SteamGames_Henry/blob/main/ETL-/1_ETL_steam_games.ipynb) , [ETL_user_reviews](http://https://github.com/paulorlv/Proyecto1_SteamGames_Henry/blob/main/ETL-/1_ETL_user_reviews.ipynb "ETL_user_reviews") y [ETL_users_items](http://https://github.com/paulorlv/Proyecto1_SteamGames_Henry/blob/main/ETL-/1_ETL_users_items.ipynb "ETL_users_items").

#### Feature engineering

Uno de los pedidos para este proyecto fue aplicar un análisis de sentimiento a los reviews de los usuarios. Para ello se creó una nueva columna llamada 'sentiment_analysis' que reemplaza a la columna que contiene los reviews donde clasifica los sentimientos de los comentarios con la siguiente escala:

0 si es malo,
1 si es neutral o esta sin review
2 si es positivo.
Dado que el objetivo de este proyecto es realizar una prueba de concepto, se realiza un análisis de sentimiento básico utilizando TextBlob que es una biblioteca de procesamiento de lenguaje natural (NLP) en Python. El objetivo de esta metodología es asignar un valor numérico a un texto, en este caso a los comentarios que los usuarios dejaron para un juego determinado, para representar si el sentimiento expresado en el texto es negativo, neutral o positivo.

Esta metodología toma una revisión de texto como entrada, utiliza TextBlob para calcular la polaridad de sentimiento y luego clasifica la revisión como negativa, neutral o positiva en función de la polaridad calculada. En este caso, se consideraron las polaridades por defecto del modelo, el cuál utiliza umbrales -0.2 y 0.2, siendo polaridades negativas por debajo de -0.2, positivas por encima de 0.2 y neutrales entre medio de ambos.

Por otra parte, y bajo el mismo criterio de optimizar los tiempos de respuesta de las consultas en la API y teniendo en cuenta las limitaciones de almacenamiento en el servicio de nube para deployar la API, se realizaron dataframes auxiliares para cada una de las funciones solicitadas. En el mismo sentido, se guardaron estos dataframes en formato parquet que permite una compresión y codificación eficiente de los datos.

Todos los detalles del desarrollo se pueden ver en [Feature_Engineering](http://https://github.com/paulorlv/Proyecto1_SteamGames_Henry/blob/main/Feature_Engineering.ipynb "Feature_Engineering").
#### EDA
Se realizó el EDA a los tres conjuntos de datos sometidos a ETL con el objetivo de identificar las variables que se pueden utilizar en la creación del modelo de recmendación. Para ello se utilizó la librería Pandas para la manipulación de los datos y las librerías Matplotlib y Seaborn para la visualización.

En particular para el modelo de recomendación, se terminó eligiendo construir un dataframe específico con el id del usuario que realizaron reviews, los nombres de los juegos a los cuales se le realizaron comentarios y una columna de rating que se construyó a partir de la combinación del análisis de sentimiento y la recomendación a los juegos.

El desarrollo de este análisis se encuentra en [EDA](http://https://github.com/paulorlv/Proyecto1_SteamGames_Henry/tree/main/EDA "EDA")

#### Modelo de aprendizaje automático
Se crearon dos modelos de recomendación, que generan cada uno, una lista de 5 juegos ya sea ingresando el nombre de un juego o el id de un usuario.

En el primer caso, el modelo tiene una relación ítem-ítem, esto es, se toma un juego y en base a que tan similar es ese juego con el resto de los juegos se recomiendan similares. En el segundo caso, el modelo aplicar un filtro usuario-juego, es decir, toma un usuario, encuentra usuarios similares y se recomiendan ítems que a esos usuarios similares les gustaron.

Para generar estos modelos se adoptaron algoritmos basados en la memoria, los que abordan el problema del filtrado colaborativo utilizando toda la base de datos, tratando de encontrar usuarios similares al usuario activo (es decir, los usuarios para los que se les quiere recomendar) y utilizando sus preferencias para predecir las valoraciones del usuario activo.

Para medir la similitud entre los juegos (item_similarity) y entre los usuarios (user_similarity) se utilizó la similitud del coseno que es una medida comúnmente utilizada para evaluar la similitud entre dos vectores en un espacio multidimensional. En el contexto de sistemas de recomendación y análisis de datos, la similitud del coseno se utiliza para determinar cuán similares son dos conjuntos de datos o elementos, y se calcula utilizando el coseno del ángulo entre los vectores que representan esos datos o elementos.

El desarrollo para la creación de los dos modelos se presenta en [Modelo](htthttps://github.com/paulorlv/Proyecto1_SteamGames_Henry/tree/main/Modelo "Modelo")

#### Desarrollo de API
Para el desarrolo de la API se decidió utilizar el framework FastAPI, creando las siguientes funciones:
- **developer(desarrollador: str)**: Retorna la cantidad de ítems y el porcentaje de contenido gratis por año para un desarrollador dado.
- **userdata(User_id: str)**: Retorna el dinero gastado, cantidad de ítems y el porcentaje de comentarios positivos en la revisión para un usuario dado.
- **UserForGenre(género: str)**: Retorna al usuario que acumula más horas para un género dado y la cantidad de horas por año.
- **best_developer_year(año: int)**: Retorna los tres desarrolladores con más juegos recomendados por usuarios para un año dado.
- **developer_rec(desarrolladora: str)**: Retorna una lista con la cantidad de usuarios con análisis de sentimiento positivo y negativo para un desarrollador dado.
- ** user_recommend(user:str)**: Esta función recomienda 5 juegos para un usuario especifico usando un filtro colaborativo.

#### Deployment

Para el deploy de la API se seleccionó la plataforma Render que es una nube unificada para crear y ejecutar aplicaciones y sitios web, permitiendo el despliegue automático desde GitHub. Para esto se siguieron estos pasos:

- Generación de un entorno virtual dentro de mi terminal de visual studio code, donde se instalaron todos los requerimientos necesarios para asi generar el archivo requeriments,txt que luego seria consumido por render..
- Se generó un servicio nuevo en render.com, conectado al presente repositorio y utilizando Docker como Runtime.
- Finalmente, el servicio queda corriendo en https://proyecto-steamgames-paulolara.onrender.com/docs.

#### Video
