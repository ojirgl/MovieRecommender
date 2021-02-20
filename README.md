# MovieRecommender
Recommender system of movies based on content clustering and collaborative filtering in different ways. Then, by combining different techniques, we prodce 4 different personalized lists to each user.

CÓMO EJECUTAR Y CONSTRUIR:

OPCIONAL:

Despliegue de una base de datos:
	- en la carpeta sql/tvapp/ se encuentra DEPLOY_TVAPP.sql con el código de creación de la BBDD
	- no se olvide de cambiar la ruta en el comando COPY
	- los archivos con datos se encuentran en la carpeta sql/tvapp/data/
- esta parte es opcional ya que se puede leer directamente archivos ya procesados 

EJECUCIÓN:

- el código se encuentra en python/RakutenMovieRecommender.ipynb
- para utilizar ya archivos preprocesados sin tener la BBDD se puede usar los que están en python/data/
- en el código del notebook están las celdas de lectura de los mismos
- se requiere el archivo sql_lib.py si es que se ha desplegado una BBDD previamente
- enviroment.props de momento sigue sólo para la conexión a a BBDD, después contendrá todos los parámetros de modelos etc.
- simplemente ejecutando todo en orden se debería producir el resultado esperado en la última celda
- el código está con comentarios pero muy pobres - estoy trabajando ya en la versión escrita del TFM donde explico cada paso y librería utilizada

- mi idea era agrupar el contenido en clústeres basados en contenido y después
con lo poco que recibimos de backend crear una "fuerza de relación" entre el usuario y película 
(se basa en % de película visto, si ha sido un pedido, trailer etc., combinado con el rating de imdb)
- ya que poseo de todo esto, decidí directamente crear varias listas de películas recomendadas - en este caso copiando a Netflix
- las listas están mencionadas en el código en el apartado más abajo con título "Listas de recomendación"
- cada lista tiene un algoritmo específico (o más bien combinación de algoritmos) para conseguir lo más atractivo para el usuario
