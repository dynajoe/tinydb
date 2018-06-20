SELECT x.name, *
FROM person, person p, person x
WHERE person.name = 'joe' AND p.name = 'stephen' AND x.name = 'justin' OR x.name = 'justin'