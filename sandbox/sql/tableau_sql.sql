drop table if exists warehouse.tableau_data;

Create table warehouse.tableau_data as
SELECT source.show_id_1, source.show_id_2, source.similarity, 'source' as Type, xy.x as X,
       xy.Y as Y, meta.show_name as Name, si.show_name, si.show_description, si.publisher, si.average_episode_length, si.number_of_episodes,re.category,re.authors,xy.community,CONCAT('https://open.spotify.com/show/', source.show_id_1) AS URL
FROM warehouse.all_similarity_matrix as source
JOIN warehouse.tableau_coordinates as xy
ON source.show_id_1 = xy.show_id
JOIN warehouse.show_info as si
ON source.show_id_1=si.show_id
JOIN datalake.rss_extract as re
ON source.show_id_1=re.show_id
JOIN warehouse.podcast_metadata as meta
ON source.show_id_1 = meta.show_id
WHERE source.similarity > 0.5
UNION
SELECT target.show_id_1, target.show_id_2, target.similarity, 'target' as Type, xy.x as X,
       xy.Y as Y, meta.show_name as Name, si.show_name, si.show_description, si.publisher, si.average_episode_length, si.number_of_episodes,re.category, re.authors,xy.community,CONCAT('https://open.spotify.com/show/', target.show_id_2) AS URL
FROM warehouse.all_similarity_matrix as target
JOIN warehouse.tableau_coordinates as xy
ON target.show_id_2 = xy.show_id
JOIN warehouse.show_info as si
ON target.show_id_2=si.show_id
JOIN datalake.rss_extract as re
ON target.show_id_2=re.show_id
JOIN warehouse.podcast_metadata as meta
ON target.show_id_2 = meta.show_id
WHERE target.similarity > 0.5