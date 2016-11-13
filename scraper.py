import datetime
import hashlib
from dc_base_scrapers.common import save
from dc_base_scrapers.hashonly_scraper import HashOnlyScraper
from lxml import etree


class DaventryHashOnlyScraper(HashOnlyScraper):

    def store_history(self, data_str, council_id):
        """
        KML from Daventry contains style information which is generated
        dynamically on each request, meaning we get a different hash every time
        even if the data has not changed.
        This custom store_history() method strips the style data from the KML
        and re-serialises it to ensure we get consistent hash values if the
        data is the same and only the style info has changed.
        """
        tree = etree.fromstring(data_str)
        elements = tree.findall('{http://www.opengis.net/kml/2.2}Document')

        # strip all the style tags out of the kml
        for element in elements[0]:
            if element.tag == '{http://www.opengis.net/kml/2.2}Style':
                element.getparent().remove(element)
        # re-serialise without style tags
        data_str = etree.tostring(tree)

        hash_record = {
            'council_id': council_id,
            'timestamp': datetime.datetime.now(),
            'table': self.table,
            'content_hash': hashlib.sha1(data_str).hexdigest(),
        }
        if self.store_raw_data:
            hash_record['raw_data'] = data_str

        save(['timestamp'], hash_record, 'history')


stations_url = "http://feeds.getmapping.com/47733.wmsx?login=70a1b086-b9cc-4b22-9b90-7d88bd589d4b&password=sjy2869b&LAYERS=daventry_parliamentary_polling_stations&TRANSPARENT=TRUE&HOVER=false&FORMAT=kml&SERVICE=WMS&VERSION=1.3.0&REQUEST=GetMap&CRS=EPSG%3A27700&BBOX=449708,250805,486927,287603&WIDTH=867&HEIGHT=426"
districts_url = "http://feeds.getmapping.com/47732.wmsx?login=61dfb362-893b-464d-8a96-fb3edb7565f8&password=yd5v5y03&LAYERS=daventry_parliamentary_polling_districts_region&TRANSPARENT=TRUE&HOVER=false&FORMAT=kml&SERVICE=WMS&VERSION=1.3.0&REQUEST=GetMap&CRS=EPSG%3A27700&BBOX=447025,247876,489247,288998&WIDTH=867&HEIGHT=426"
council_id = 'E07000151'


stations_scraper = DaventryHashOnlyScraper(stations_url, council_id, 'stations')
stations_scraper.scrape()
districts_scraper = DaventryHashOnlyScraper(districts_url, council_id, 'districts')
districts_scraper.scrape()
