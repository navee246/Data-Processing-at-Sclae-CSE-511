/*
1. Write a SQL query to return the total number of movies
for each genre. Your query result should be saved in a table called “query1” which has two attributes:
“name” attribute is a list of genres, and “moviecount” list of movie counts for each genre.
*/

DROP TABLE IF EXISTS temp1;
CREATE TABLE temp1 AS
SELECT genreid AS genreid, COUNT(movieid) AS moviecount
FROM hasagenre
GROUP BY genreid;

DROP TABLE IF EXISTS query1;
CREATE TABLE query1 AS
SELECT a.name AS name, b.moviecount as moviecount
FROM genres a, temp1 b
WHERE a.genreid = b.genreid;



/*
2. Write a SQL query to return the average rating per genre. 
Your query result should be saved in a table called “query2” which has two attributes: name, rating
*/

DROP TABLE IF EXISTS query2;
CREATE TABLE query2 AS
SELECT genres.name as name, AVG(p.rating) as rating
FROM genres
INNER JOIN ( SELECT ratings.rating, hasagenre.genreid
            FROM hasagenre, ratings
			WHERE hasagenre.movieID = ratings.movieid
		  ) p
ON genres.genreid = p.genreid
GROUP BY genres.genreid;



/*
3. Write a SQL query to return the movies have at least 10 ratings. 
Your query result should be saved in a table called “query3” which has two attributes: title, CountOfRatings.
*/

DROP TABLE IF EXISTS query3;
CREATE TABLE query3 AS
SELECT movies.title as title, COUNT(ratings.rating) as CountOfRatings
FROM movies, ratings
WHERE movies.movieid = ratings.movieid
GROUP BY movies.movieid
HAVING COUNT(movies.movieid)>= 10;

/*
4. Write a SQL query to return all “Comedy” movies, including movieid and title. 
Your query result should be saved in a table called “query4” which has two attributes, movieid and title.
*/

DROP TABLE IF EXISTS query4;
CREATE TABLE query4 AS
SELECT p.movieid as movieid  , p.title as title
FROM genres
INNER JOIN ( SELECT movies.movieid , movies.title , hasagenre.genreid
			FROM movies, hasagenre
            WHERE hasagenre.movieid = movies.movieid 
		  ) p
ON genres.genreid = p.genreid
WHERE genres.name = 'Comedy';



/*
5. Write a SQL query to return the average rating per movie. 
 Your query result should be saved in a table called “query5” which has two attributes, title and average.
*/

DROP TABLE IF EXISTS query5;
CREATE TABLE query5 AS
SELECT movies.title as title, p.average as average
FROM movies
INNER JOIN ( SELECT AVG(ratings.rating) as average, ratings.movieid
            FROM ratings 
            GROUP BY ratings.movieid) p
ON movies.movieid = p.movieid;

/*
6. Write a SQL query to return the average rating for all “Comedy” movies. 
Your query result should be saved in a table called “query6” which has one attribute, average.
*/

DROP TABLE IF EXISTS query6;
CREATE TABLE query6 AS
SELECT AVG(ratings.rating) as average
from genres, hasagenre, ratings
WHERE genres.name = 'Comedy' AND  genres.genreid = hasagenre.genreid AND hasagenre.movieid = ratings.movieid
GROUP BY genres.name;



/*
7. Write a SQL query to return the average rating for all movies and each of these movies is both “Comedy” and “Romance”. 
Your query result should be saved in a table called “query7” which has one attribute, average.
*/


DROP TABLE IF EXISTS query7;
CREATE table query7 AS
SELECT AVG(ratings.rating) as average
FROM ratings
INNER JOIN (
    SELECT hasagenre.movieid
    FROM hasagenre
    INNER JOIN genres ON hasagenre.genreid = genres.genreid
    WHERE genres.Name IN ('Comedy', 'Romance')
    GROUP BY hasagenre.movieid
    HAVING COUNT(DISTINCT genres.Name) = 2
) m 
ON ratings.movieid = m.movieid;


/*
8.Write a SQL query to return the average rating for all movies and each of these movies is “Romance” but not “Comedy”. 
Your query result should be saved in a table called “query8” which has one attribute, average.
*/


DROP TABLE IF EXISTS query8;
CREATE table query8 AS
SELECT AVG(ratings.rating) as average
from ratings
WHERE ratings.movieid IN ( SELECT movieid
						   FROM ( SELECT movieid
								   FROM hasagenre, genres
							       WHERE genres.genreid = hasagenre.genreid AND genres.name = 'Romance' ) p
						  WHERE movieid NOT IN ( SELECT movieid
								                 FROM hasagenre, genres
							                     WHERE genres.genreid = hasagenre.genreid AND genres.name = 'Comedy' ) );





/*
9. Find all movies that are rated by a user such that the userId is equal to v1.
The v1 will be an integer parameter passed to the SQL query. Your query result
should be saved in a table called “query9” which has two attributes: “movieid”
is a list of movieid’s rated by userId v1, and “rating” is a list of ratings
given by userId v1 for corresponding movieid.
*/


DROP TABLE IF EXISTS query9;
CREATE TABLE query9  AS
SELECT ratings.movieid, ratings.rating
FROM ratings
WHERE ratings.userid = :v1;

















