from podcast_discovery.data.upload import RSS, Transcripts
from podcast_discovery.keywords.keyword_extraction import Keywords
from podcast_discovery.visualization.similarity_matrix import SimilarityMatrix
from podcast_discovery.visualization.coordinates import Coordinates

class Pipeline:

    def __init__(self):
        self.transcripts = Transcripts()
        self.rss = RSS()
        self.keywords = Keywords()
        self.similarity = SimilarityMatrix()
        self.coordinates = Coordinates()

    def run(self):
        print('uploading transcripts...')
        #self.transcripts.multi_thread_import()
        print('transcripts successfully uploaded to datalake.sample_podcast_transcripts')

        print('uploading rss feeds...')
        #self.rss.multi_thread_import()
        print('transcripts successfully uploaded to datalake.sample_rss_extracts')

        print('extracting keywords...')
        #self.keywords.run()
        print('keywords successfully uploaded to datalake.sample_shows_and_keywords')

        print('building similarity matrix...')
        self.similarity.run()

        print('generating coordinates...')
        self.coordinates.generate('sample_tableau_coordinates', 0.1, 'kamada',
                                  schema='datalake')






if __name__ == '__main__':
    pipeline = Pipeline()
    pipeline.run()

