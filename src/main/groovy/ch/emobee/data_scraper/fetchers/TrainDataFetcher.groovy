package ch.emobee.data_scraper.fetchers

import ch.emobee.data_scraper.services.MongoDBService
import ch.emobee.data_scraper.utils.DataFormatUtils
import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import groovy.json.internal.LazyMap

import java.text.SimpleDateFormat
import java.util.logging.Logger


abstract class TrainDataFetcher {

//    private static final String DOWNLOAD_URL = "https://data.sbb.ch/explore/dataset/ist-daten-sbb/download/?format=json&timezone=Europe/Berlin"
    private final _logger = Logger.getLogger(TrainDataFetcher.toString())
    final String OUTPUT_PATH = './output/source_data/'

    TrainDataFetcher() {
    }

    abstract def extractData(List<LazyMap> parsedData)

    void fetchData(downloadUrl, fileName) {
        _logger.info("Start fetching data from $downloadUrl")
        new DataFormatUtils().exportToFile(new URL(downloadUrl).getText(), fileName, OUTPUT_PATH)
        _logger.info("Data fetched and file was exported.")
    }

    def parseData(String fileName) {
        File file = new File("$OUTPUT_PATH$fileName")
        def jsonSlurper = new JsonSlurper()
        def parsedData = jsonSlurper.parseText(file.text)

        _logger.info("Parsed file $OUTPUT_PATH$fileName.")
        return parsedData
    }

    void persistData(data, databaseName, collectionName) {
        MongoDBService.save(data, databaseName, collectionName)
        _logger.info("Persisted data.")
    }

    static def createExtractObject(parsedData, adjustedRecords) {
        def date = new Date()
        def sdf = new SimpleDateFormat("yyyy-MM-dd")

        def extractedData = [
                "fetchDate"      : sdf.format(date),
                "numberOfRecords": parsedData.size(),
                "records"        : adjustedRecords
        ]
        return extractedData
    }
}
