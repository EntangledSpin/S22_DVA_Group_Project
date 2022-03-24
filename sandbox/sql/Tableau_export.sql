SELECT source.show_id_1, source.show_id_2, source.similarity, 'source' as Type, xy.x as X,
       xy.Y as Y, meta.show_name as Name, CONCAT('https://open.spotify.com/show/', source.show_id_1) AS URL
FROM datalake.similarity_matrix as source
JOIN datalake.tableau_coordinates as xy
ON source.show_id_1 = xy.show_id
JOIN warehouse.podcast_metadata as meta
ON source.show_id_1 = meta.show_id
UNION
SELECT target.show_id_1, target.show_id_2, target.similarity, 'target' as Type, xy.x as X,
       xy.Y as Y, meta.show_name as Name, CONCAT('https://open.spotify.com/show/', target.show_id_2) AS URL
FROM datalake.similarity_matrix as target
JOIN datalake.tableau_coordinates as xy
ON target.show_id_2 = xy.show_id
JOIN warehouse.podcast_metadata as meta
ON target.show_id_2 = meta.show_id
