SELECT show_id_1 AS Source, show_id_2 AS Target, 'Undirected' AS Type, 1 AS Weight, similarity
FROM datalake.similarity_matrix
WHERE similarity > 0.2
