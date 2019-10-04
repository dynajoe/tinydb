CREATE TABLE IF NOT EXISTS person (
    name text PRIMARY KEY,
    company_id int
);

CREATE TABLE IF NOT EXISTS company (
    company_id int PRIMARY KEY,
    company_name text
);

INSERT INTO company (company_id, company_name) VALUES ('1', 'Illuminate');

INSERT INTO company (company_id, company_name) VALUES ('2', 'Google');

INSERT INTO person (name, company_id) VALUES ('Joe', '1');

INSERT INTO person (name, company_id) VALUES ('Alex', '1');

INSERT INTO person (name, company_id) VALUES ('Blake', '2');

SELECT c.company_name, person.name
FROM person, company c
WHERE c.company_id = person.company_id
   AND (c.company_name = 'Google');