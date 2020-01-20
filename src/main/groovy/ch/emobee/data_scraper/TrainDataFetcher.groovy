package ch.emobee.data_scraper

import ch.emobee.data_scraper.services.MongoDBService
import ch.emobee.data_scraper.utils.DataFormatUtils
import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import groovy.json.internal.LazyMap

import java.text.SimpleDateFormat
import java.util.logging.Logger


class TrainDataFetcher {

    private static final String DOWNLOAD_URL = "https://data.sbb.ch/explore/dataset/ist-daten-sbb/download/?format=json&timezone=Europe/Berlin"
    private final logger = Logger.getLogger(TrainDataFetcher.toString())

    void start() {

        def date = new Date()
        def sdf = new SimpleDateFormat("yyyy-MM-dd")
        String fileNameRaw = "${sdf.format(date)}-raw.json"
        String fileNameExtract = "${sdf.format(date)}-extract.json"
        String path = './output/source_data/'

        _fetchData(path, fileNameRaw)
        def parsedData = _parseData(path, fileNameRaw)
        def extractedData = _extractData(parsedData as List<LazyMap>, path, fileNameExtract)
        _persistData(extractedData)
        logger.info('Finished Train Delay Fetch.')
    }

    private void _fetchData( path, fileName) {
        logger.info("Start fetching data from $DOWNLOAD_URL")
        new DataFormatUtils().exportToFile(new URL(DOWNLOAD_URL).getText(), fileName, path)
        logger.info("Data fetched and file was exported.")
    }

    private def _extractData(List<LazyMap> parsedData, String fileName, String path) {

        def date = new Date()
        def sdf = new SimpleDateFormat("yyyy-MM-dd")
        def adjustedRecords = []

        parsedData.each {
            adjustedRecords.add([
                    "line"             : it.fields["linien_id"],
                    "station"          : it.fields["haltestellen_name"],
                    "vehicle"          : it.fields["verkehrsmittel_text"],
                    "scheduled_arrival": it.fields["ankunftszeit"],
                    "actual_arrival"   : it.fields["an_prognose"],
                    "delay_at_arrival" : it.fields["ankunftsverspatung"],
                    "cancelled"        : it.fields["faellt_aus_tf"],
                    "service_provider" : it.fields["betreiber_name"],
                    "geoPosition"      : it.fields["geopos"],
            ])
        }

        def extractedData = [
                "fetchDate"      : sdf.format(date),
                "numberOfRecords": parsedData.size(),
                "records"        : adjustedRecords
        ]

        def json = JsonOutput.toJson(extractedData)
        new DataFormatUtils().exportToFile(json, path, fileName)

        logger.info("Extract was created and exported.")
        return extractedData
    }

    private def _parseData(String path, String fileName) {
        File file = new File("$path$fileName")
        def jsonSlurper = new JsonSlurper()
        def parsedData = jsonSlurper.parseText(file.text)

        logger.info("Parsed file $path$fileName.")
        return parsedData
    }

    private void _persistData(data) {
        MongoDBService.save(data)
        logger.info("Persisted data.")
    }
}
