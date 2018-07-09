SELECT c.company_name, person.name, person.favorite_number
FROM person, company c
WHERE c.company_id = person.company_id + 1
   AND (c.company_name = 'Softek' AND c.company_name = 'Google');