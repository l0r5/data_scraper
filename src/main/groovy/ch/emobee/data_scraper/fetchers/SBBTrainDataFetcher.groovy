package ch.emobee.data_scraper.fetchers

import ch.emobee.data_scraper.utils.DataFormatUtils
import groovy.json.JsonOutput
import groovy.json.internal.LazyMap

import java.text.SimpleDateFormat
import java.util.logging.Logger

class SBBTrainDataFetcher extends TrainDataFetcher {

    private static final String DOWNLOAD_URL = "https://data.sbb.ch/explore/dataset/ist-daten-sbb/download/?format=json&timezone=Europe/Berlin"
    private final _logger = Logger.getLogger(SBBTrainDataFetcher.toString())

    def date = new Date()
    def sdf = new SimpleDateFormat("yyyy-MM-dd")
    def fileNameRaw = "SBB-${sdf.format(date)}-raw.json"
    def fileNameExtract = "SBB-${sdf.format(date)}-extract.json"

    void runCompleteRoutine() {
        super.fetchData(DOWNLOAD_URL,fileNameRaw)
        def parsedData = parseData(fileNameRaw) as List<LazyMap>
        def extractedData = extractData(parsedData)
        super.persistData(extractedData, "sbb-train-delays", "extract-scheduled-actual-arrival")
    }

    @Override
    def extractData(List<LazyMap> parsedData) {

        def adjustedRecords = []

        parsedData.each {
            adjustedRecords.add([
                    "line"             : it.fields["linien_id"],
                    "station"          : it.fields["haltestellen_name"],
                    "vehicle"          : it.fields["verkehrsmittel_text"],
                    "scheduled_arrival": it.fields["ankunftszeit"],
                    "actual_arrival"   : it.fields["an_prognose"],
                    "delay_at_arrival" : it.fields["ankunftsverspatung"],
                    "cancelled"        : it.fields["faellt_aus_tf"]
            ])
        }
        def extractedData = createExtractObject(parsedData, adjustedRecords)
        def json = JsonOutput.toJson(extractedData)

        DataFormatUtils.exportToFile(json, OUTPUT_PATH, fileNameExtract)
        _logger.info("Extract was created and exported.")
        return extractedData
    }


}
