package ch.emobee.data_scraper

import ch.emobee.data_scraper.services.MongoDBService
import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import groovy.json.internal.LazyMap

import java.text.SimpleDateFormat;


public class TrainDataFetcher {

    private static final String DOWNLOAD_URL = "https://data.sbb.ch/explore/dataset/ist-daten-sbb/download/?format=json&timezone=Europe/Berlin"
    private String fileName = ''

    public void start() {
        def dataToPersist = []
        _fetchData()
        def parsedData = _parseData()
        def extractedData = _extractData(parsedData as List<LazyMap>)
        _persistData(extractedData)
        print 'Done'
    }

    private void _fetchData() {

        List<String> fetchedData = []
        def jsonSlurper = new JsonSlurper()

        println "Start fetching data..."

        def date = new Date()
        def sdf = new SimpleDateFormat("yyyy-MM-dd")

        fileName = "${sdf.format(date)}-train-delay-data.json"
        new File("./output/$fileName") << new URL(DOWNLOAD_URL).getText()

        println "Data fetched and stored under ./output/$fileName"
    }

    private def _parseData() {

        println "Start formatting data..."

        def jsonSlurper = new JsonSlurper()
        File file = new File("./output/$fileName")
        def parsedData = jsonSlurper.parseText(file.text)

        return parsedData
    }

    private def _extractData(List<LazyMap> parsedData) {

        def date = new Date()
        def sdf = new SimpleDateFormat("yyyy-MM-dd")

        println "Start extracting data..."

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

        println "Create extract under ./output/${sdf.format(date)}-extract.json"
        new File("./output/${sdf.format(date)}-extract.json").write(json)
        return extractedData
    }

    private static void _persistData(data) {
        println 'Persist data...'
        MongoDBService.save(data)
    }
}
