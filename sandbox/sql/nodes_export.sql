SELECT show_id_1 as Node FROM datalake.similarity_matrix
WHERE similarity > 0.2
UNION
SELECT show_id_2 as Node FROM datalake.similarity_matrix
WHERE similarity > 0.2
