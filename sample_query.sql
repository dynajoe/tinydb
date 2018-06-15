CREATE TABLE person (
   name int,
   favorite_number text,
);

INSERT INTO person ( name, favorite_number ) VALUES ( joe, 1337 );
INSERT INTO person ( name, favorite_number ) VALUES ( justin, 31337 );
INSERT INTO person ( name, favorite_number ) VALUES ( stephen, 42 );

SELECT * FROM person;