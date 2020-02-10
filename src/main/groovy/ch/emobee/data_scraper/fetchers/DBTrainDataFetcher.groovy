package ch.emobee.data_scraper.fetchers

import ch.emobee.data_scraper.utils.DataFormatUtils
import groovy.json.JsonOutput
import groovy.json.internal.LazyMap

import java.util.logging.Logger

class DBTrainDataFetcher extends TrainDataFetcher {

    private final _logger = Logger.getLogger(DBTrainDataFetcher.toString())

    def fileNameRaw = "db-train-delays-2019.json"
    def fileNameExtract = "db-train-delays-2019-extract.json"

    void runCompleteRoutine() {
        def parsedData = parseData(fileNameRaw) as List<LazyMap>
        def extractedData = extractData(parsedData)
        super.persistData(extractedData, "db-train-delays", "extract-probabilities-2019")
    }

    @Override
    def extractData(List<LazyMap> parsedData) {

        def adjustedRecords = []

        parsedData.each {
            adjustedRecords.add([
                    "train_number"          : it["train_number"],
                    "connection"            : it["connection"],
                    "percentage_punctuality": it["percentage_punctuality"],
                    "average_last_delay"    : it["average_last_delay"],
                    "average_max_delay"     : it["average_max_delay"],
            ])
        }

        def extractedData = createExtractObject(parsedData, adjustedRecords)
        def json = JsonOutput.toJson(extractedData)

        DataFormatUtils.exportToFile(json, OUTPUT_PATH, fileNameExtract)

        _logger.info("Extract was created and exported.")
        return extractedData
    }

}
