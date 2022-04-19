from podcast_discovery.data.upload import RSS, Transcripts
from podcast_discovery.keywords.keyword_extraction import Keywords
from podcast_discovery.visualization.similarity_matrix import SimilarityMatrix
from podcast_discovery.visualization.coordinates import Coordinates
from podcast_discovery.db_core.database import Database






class Pipeline:

    def __init__(self):
        self.transcripts = Transcripts()
        self.rss = RSS()
        self.keywords = Keywords()
        self.similarity = SimilarityMatrix()
        self.coordinates = Coordinates()
        self.database = Database()

    def run(self):
        print('uploading transcripts...')
        #self.transcripts.multi_thread_import(schema='datalake', table='sample_podcast_transcripts')


        print('uploading rss feeds...')
        #self.rss.multi_thread_import(schema='datalake', table='sample_podcast_rss')


        print('extracting keywords...')
        self.keywords.run()


        print('building similarity matrix...')
        self.similarity.run()

        print('generating coordinates...')
        self.coordinates.generate()






if __name__ == '__main__':
    pipeline = Pipeline()
    pipeline.run()

