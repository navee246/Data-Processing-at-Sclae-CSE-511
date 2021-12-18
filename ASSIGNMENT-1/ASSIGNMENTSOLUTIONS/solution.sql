/*
User table
*/

CREATE TABLE users (
	userid int PRIMARY KEY NOT NULL,
	name text NOT NULL
);

/*
Movies Table
*/

CREATE TABLE movies
(
	movieid int PRIMARY KEY NOT NULL,
	title text NOT NULL
   
);

/*
Taginfo Table
*/

CREATE TABLE taginfo
(
	tagid int PRIMARY KEY NOT NULL,
    	Content text NOT NULL
   
);

/*
genres Table
*/

CREATE TABLE genres
(
	genreid int PRIMARY KEY NOT NULL,
    	name text NOT NULL
);

/*
ratings Table
*/

CREATE TABLE ratings
(
	userid int REFERENCES users(userid),
    	movieid int REFERENCES movies(movieid),
    	rating numeric NOT NULL CHECK (rating>=0 AND rating<=5),
	timestamp BIGINT NOT NULL,
	PRIMARY KEY (userid,movieid)
   
);

/*
tags Table
*/

CREATE TABLE tags
(
	userid int REFERENCES users(userid),
    	movieid int REFERENCES movies(movieid),
    	tagid int REFERENCES taginfo(tagid),
    	timestamp bigint NOT NULL,
	PRIMARY KEY (userid,movieid,tagid)
);

/*
has a genre Table
*/

CREATE TABLE hasagenre
(
	movieid int REFERENCES movies(movieid),  
    	genreid int REFERENCES genres(genreid),
	PRIMARY KEY (movieid,genreid)
);

drop table hasagenre;
drop table tags;
drop table movies;
drop table ratings;
drop table genres;
drop table taginfo;
drop table users;


select * from movies;


/*
End of Tables
*/

