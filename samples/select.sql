SELECT person.name
FROM person
WHERE person.name = 'Joe';

SELECT c.company_name, person.name
FROM person, company c
WHERE c.company_id = person.company_id
  AND c.company_name = 'Google';