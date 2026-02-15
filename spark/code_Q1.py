from pyspark import SparkContext
from datetime import datetime

# Initialisation du contexte Spark
sc = SparkContext(appName="Films_Frequents_Cinq_Dernieres_Annees")

# Chargement du fichier WatchedMovies.txt en tant que RDD
watched_movies_rdd = sc.textFile("WatchedMovies.txt")

# Filtrage des lignes liées aux cinq dernières années
filtered_movies_rdd = watched_movies_rdd.filter(lambda line: "2015/09/17" <= line.split(",")[2] <= "2020/09/16")

# Transformation en paire clé-valeur avec (MID, (Année, 1)) pour chaque ligne
movies_year_count_rdd = filtered_movies_rdd.map(lambda line: ((line.split(",")[1], line.split(",")[2][:4]), 1))

# Réduction par clé pour compter le nombre de fois que chaque film a été regardé chaque année
movies_year_count_sum_rdd = movies_year_count_rdd.reduceByKey(lambda x, y: x + y)

# Filtrage des films qui ont été regardés au moins 1000 fois au cours d'une année
selected_movies_rdd = movies_year_count_sum_rdd.filter(lambda x: x[1] >= 3)

# Transformation pour obtenir le format (MID, Année)
result_rdd = selected_movies_rdd.map(lambda x: (x[0][0], x[0][1]))

# Affichage des résultats
result_rdd.foreach(print)

# Enregistrement des résultats dans un fichier de sortie (vous pouvez ajuster le chemin selon vos besoins)
result_rdd.saveAsTextFile("out_part2_Q2")

# Arrêt du contexte Spark
sc.stop()
