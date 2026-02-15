from pyspark import SparkContext, SparkConf

# Create a Spark configuration and set the application name
conf = SparkConf().setAppName("MostPopularMoviesByYear")
sc = SparkContext(conf=conf)

# Load the WatchedMovies data from the input file
watched_movies = sc.textFile("WatchedMovies.txt")

# Split each line into fields
# Assuming the format is "Username, MID, StartTimestamp, EndTimestamp"
# Adjust the split logic based on your actual data format
watched_movies_data = watched_movies.map(lambda line: line.split(","))

# Map the data to (MID, (Year, User)) pairs
movie_year_user_pairs = watched_movies_data.map(lambda fields: ((fields[1], fields[2].split("/")[0]), fields[0]))

# Remove duplicate users in a specific year for each movie
distinct_users_per_year = movie_year_user_pairs.distinct()

# Map to (MID, 1) to count occurrences of each movie in each year
movie_count = distinct_users_per_year.map(lambda x: ((x[0][0], x[0][1]), 1))

# Reduce by key to get the count of distinct users for each movie in each year
movie_user_count = movie_count.reduceByKey(lambda x, y: x + y)

# Map to (Year, (MID, Count)) to find the most popular movie for each year
most_popular_movies = movie_user_count.map(lambda x: (x[0][1], (x[0][0], x[1])))

# Reduce by key to get the final result, selecting the most popular movie for each year
result = most_popular_movies.reduceByKey(lambda x, y: x if x[1] > y[1] else y).map(lambda x: (x[0], x[1][0]))

# Collect the result to the driver program
collected_result = result.collect()

# Count the occurrences of each MID
mid_counts = {}
for _, mid in collected_result:
    mid_counts[mid] = mid_counts.get(mid, 0) + 1

# Filter MIDs that are present in at least 2 years
filtered_mids = [mid for mid, count in mid_counts.items() if count >= 2]

# Filter the collected result based on the selected MIDs
final_result = [(year, mid) for year, mid in collected_result if mid in filtered_mids]

# Save the result to the output directory
sc.parallelize(final_result).map(lambda x: f"{x[1]}").saveAsTextFile("OutPart2_Q2")

# Stop the Spark context
sc.stop()