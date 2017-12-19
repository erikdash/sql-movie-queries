import sqlite3 as lite
import csv
import re
import pandas as pd
con = lite.connect('cs1656.sqlite')

with con:
	cur = con.cursor()

	########################################################################
	### CREATE TABLES ######################################################
	########################################################################
	# DO NOT MODIFY - START
	cur.execute('DROP TABLE IF EXISTS Actors')
	cur.execute("CREATE TABLE Actors(aid INT, fname TEXT, lname TEXT, gender CHAR(6), PRIMARY KEY(aid))")

	cur.execute('DROP TABLE IF EXISTS Movies')
	cur.execute("CREATE TABLE Movies(mid INT, title TEXT, year INT, rank REAL, PRIMARY KEY(mid))")

	cur.execute('DROP TABLE IF EXISTS Directors')
	cur.execute("CREATE TABLE Directors(did INT, fname TEXT, lname TEXT, PRIMARY KEY(did))")

	cur.execute('DROP TABLE IF EXISTS Cast')
	cur.execute("CREATE TABLE Cast(aid INT, mid INT, role TEXT)")

	cur.execute('DROP TABLE IF EXISTS Movie_Director')
	cur.execute("CREATE TABLE Movie_Director(did INT, mid INT)")
	# DO NOT MODIFY - END

	########################################################################
	### READ DATA FROM FILES ###############################################
	########################################################################
	# actors.csv, cast.csv, directors.csv, movie_dir.csv, movies.csv
	# UPDATE THIS

	actors = pd.read_csv("actors.csv", sep=',', engine='python', names=['aid','fname','lname', 'gender'])
	movies = pd.read_csv("movies.csv", sep=',', engine='python', names=['mid','title','year', 'rank'])
	cast = pd.read_csv("cast.csv", sep=',', engine='python', names=['aid','mid','role'])
	directors = pd.read_csv("directors.csv", sep=',', engine='python', names=['did','fname','lname'])
	movie_director = pd.read_csv("movie_dir.csv", sep=',', engine='python', names=['did','mid'])

	########################################################################
	### INSERT DATA INTO DATABASE ##########################################
	########################################################################
	# UPDATE THIS TO WORK WITH DATA READ IN FROM CSV FILES

	actors.to_sql("Actors", con, index=False, if_exists='append')
	movies.to_sql("Movies", con, index=False, if_exists='append')
	cast.to_sql("Cast", con, index=False, if_exists='append')
	directors.to_sql("Directors", con, index=False, if_exists='append')
	movie_director.to_sql("Movie_Director", con, index=False, if_exists='append')

	con.commit()

	########################################################################
	### QUERY SECTION ######################################################
	########################################################################
	queries = {}

	# DO NOT MODIFY - START
	# DEBUG: all_movies ########################
	queries['all_movies'] = '''
	SELECT * FROM Movies
	'''
	# DEBUG: all_actors ########################
	queries['all_actors'] = '''
	SELECT * FROM Actors
	'''
	# DEBUG: all_cast ########################
	queries['all_cast'] = '''
	SELECT * FROM Cast
	'''
	# DEBUG: all_directors ########################
	queries['all_directors'] = '''
	SELECT * FROM Directors
	'''
	# DEBUG: all_movie_dir ########################
	queries['all_movie_dir'] = '''
	SELECT * FROM Movie_Director
	'''
	# DO NOT MODIFY - END

	########################################################################
	### INSERT YOUR QUERIES HERE ###########################################
	########################################################################
	# NOTE: You are allowed to also include other queries here (e.g.,
	# for creating views), that will be executed in alphabetical order.
	# We will grade your program based on the output files q01.csv,
	# q02.csv, ..., q12.csv

	# Q01 ########################
	queries['q01'] = '''
	SELECT DISTINCT Actors.fname, Actors.lname
	FROM Actors
	INNER JOIN `Cast` AS FHCast ON Actors.aid = FHCast.aid
	INNER JOIN Movies AS FHMovies ON FHCast.mid = FHMovies.mid
	INNER JOIN `Cast` AS SHCast ON Actors.aid = SHCast.aid
	INNER JOIN Movies AS SHMovies ON SHCast.mid = SHMovies.mid
	WHERE FHMovies.year >= 1900
		AND FHMovies.year <= 1950
		AND SHMovies.year >= 1951
		AND SHMovies.year <= 2000
	;'''

	# Q02 ########################
	queries['q02'] = '''
	SELECT title, year
	FROM Movies
	WHERE year = (SELECT year FROM Movies WHERE title = '{}')
		AND `rank` > (SELECT `rank` FROM Movies WHERE title = '{}')
	;'''.format("Rogue One: A Star Wars Story", "Rogue One: A Star Wars Story")

	# Q03 ########################
	queries['q03'] = '''
	SELECT Actors.fname, Actors.lname
	FROM Actors
	INNER JOIN `Cast` AS C ON Actors.aid = C.aid
	INNER JOIN Movies AS M ON C.mid = M.mid
	WHERE M.title = '{}'
	'''.format("Star Wars VII: The Force Awakens")

	# Q04 ########################
	queries['q04'] = '''
	SELECT fname, lname
	FROM Actors A
	WHERE A.aid IN (SELECT C.aid FROM `Cast` C LEFT JOIN Movies AS M ON C.mid = M.mid WHERE M.year < 1985)
		AND A.aid NOT IN (SELECT C.aid FROM `Cast` C LEFT JOIN Movies AS M ON C.mid = M.mid WHERE M.year >= 1985)
	'''

	# Q05 ########################
	queries['q05'] = '''
	SELECT fname, lname, COUNT(*) AS co
	FROM Directors D
	INNER JOIN Movie_Director AS MD ON D.did = MD.did
	GROUP BY D.did
	ORDER BY co DESC
	LIMIT 20
	'''

	# Q06 ########################
	queries['q06'] = '''
	SELECT DISTINCT title, count(*) AS co
	FROM Movies M
	INNER JOIN  `Cast` AS C ON C.mid = M.mid
	GROUP BY M.mid
	ORDER BY co DESC
	LIMIT 10
	'''

	# Q07 ########################
	queries['q07'] = '''
	SELECT title, COUNT(DISTINCT FemaleA.aid) AS FemaleCo, COUNT(DISTINCT MaleA.aid) AS MaleCO
	FROM Movies M
	LEFT JOIN `Cast` AS FemaleC ON FemaleC.mid = M.mid
	LEFT JOIN Actors AS FemaleA ON FemaleC.aid = FemaleA.aid AND FemaleA.gender = 'Female'
	LEFT JOIN `Cast` AS MaleC ON MaleC.mid = M.mid
	LEFT JOIN Actors AS MaleA ON MaleC.aid = MaleA.aid AND MaleA.gender = 'Male'
	GROUP BY M.mid
	HAVING FemaleCo > MaleCo
	'''

	# Q08 ########################
	queries['q08'] = '''
	SELECT A.fname, A.lname, COUNT(DISTINCT D.did) AS DCount
	FROM Actors A
	INNER JOIN `Cast` AS C ON C.aid = A.aid
	INNER JOIN Movie_Director AS M ON M.mid = C.mid
	INNER JOIN Directors AS D ON D.did = M.did
	GROUP BY A.aid
	HAVING DCount >= 6
	'''

	# Q09 ########################
	queries['q09'] = '''
	SELECT DISTINCT fname, lname, COUNT(DISTINCT M.mid) AS co
	FROM Actors A, Movies M, `Cast` C
	WHERE A.aid = C.aid AND M.mid = C.mid AND
		A.fname LIKE 'S%' OR A.fname LIKE 's%' AND M.year = (SELECT MIN(year) FROM Movies
		AS M1, `Cast` AS C1 WHERE M1.mid = C1.mid AND C1.aid = A.aid)
	GROUP BY A.aid, M.year
	ORDER BY co DESC
	'''

	# Q10 ########################
	queries['q10'] = '''
	SELECT A.lname, M.title
	FROM Actors A, Directors D, Movies M
	INNER JOIN `Cast` AS C ON C.aid = A.aid AND C.mid = M.mid
	INNER JOIN Movie_Director AS MD ON MD.mid = M.mid AND MD.did = D.did AND MD.mid = C.mid
	WHERE A.lname = D.lname
	ORDER BY A.lname
	'''

	cur.execute('DROP VIEW IF EXISTS BaconInMovie')
	cur.execute("CREATE VIEW BaconInMovie AS \
		SELECT M.mid \
		FROM Movies M \
		INNER JOIN `Cast` AS C ON C.mid = M.mid \
		INNER JOIN Actors AS A ON A.aid = C.aid \
		WHERE A.fname = 'Kevin' AND A.lname = 'Bacon'")
	# Q11 ########################
	queries['q11'] = '''
	SELECT DISTINCT A.fname, A.lname
	FROM Actors A, BaconInMovie
	LEFT JOIN `Cast` AS B1Cast ON B1Cast.mid = BaconInMovie.mid
	LEFT JOIN Actors AS B1Act ON B1Act.aid = B1Cast.aid
	LEFT JOIN `Cast` AS B1Cast2 ON B1Cast2.aid = B1Act.aid
	LEFT JOIN `Cast` AS B2Cast ON B2Cast.mid = B1Cast2.mid AND B2Cast.aid != B1Cast2.aid AND B2Cast.mid != BaconInMovie.mid
	WHERE A.aid = B2Cast.aid'''

	# Q12 ########################
	queries['q12'] = '''
	SELECT A.fname, A.lname, COUNT(M.mid), AVG(M.rank)
	FROM Actors A
	INNER JOIN `Cast` AS C ON C.aid = A.aid
	INNER JOIN Movies AS M ON M.mid = C.mid
	GROUP BY A.aid
	ORDER BY AVG(M.rank) DESC
	LIMIT 20
	'''


	########################################################################
	### SAVE RESULTS TO FILES ##############################################
	########################################################################
	# DO NOT MODIFY - START
	for (qkey, qstring) in sorted(queries.items()):
		try:
			cur.execute(qstring)
			all_rows = cur.fetchall()

			print ("=========== ",qkey," QUERY ======================")
			print (qstring)
			print ("----------- ",qkey," RESULTS --------------------")
			for row in all_rows:
				print (row)
			print (" ")

			save_to_file = (re.search(r'q0\d', qkey) or re.search(r'q1[012]', qkey))
			if (save_to_file):
				with open(qkey+'.csv', 'w') as f:
					writer = csv.writer(f)
					writer.writerows(all_rows)
					f.close()
				print ("----------- ",qkey+".csv"," *SAVED* ----------------\n")

		except lite.Error as e:
			print ("An error occurred:", e.args[0])
	# DO NOT MODIFY - END
