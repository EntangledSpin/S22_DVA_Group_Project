SELECT show_id_1 AS Source, show_id_2 AS Target, 'Undirected' AS Type, 1 AS Weight, similarity, show_name_1, show_name_2
FROM datalake.similarity_matrix_with_names
WHERE similarity > 0
