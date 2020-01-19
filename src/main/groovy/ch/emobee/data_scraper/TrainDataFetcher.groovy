package ch.emobee.data_scraper

import groovy.json.JsonSlurper
import groovy.json.internal.LazyMap

import java.text.SimpleDateFormat;


public class TrainDataFetcher {

    // documentation and query builder can be found here
    // https://data.sbb.ch/explore/dataset/ist-daten-sbb/api/?dataChart=eyJxdWVyaWVzIjpbeyJjaGFydHMiOlt7InR5cGUiOiJsaW5lIiwiZnVuYyI6IkNPVU5UIiwieUF4aXMiOiJsaW5pZW5faWQiLCJzY2llbnRpZmljRGlzcGxheSI6dHJ1ZSwiY29sb3IiOiJyYW5nZS1BY2NlbnQifV0sInhBeGlzIjoiYW5rdW5mdHN6ZWl0IiwibWF4cG9pbnRzIjoyMCwidGltZXNjYWxlIjoiIiwic29ydCI6IiIsInNlcmllc0JyZWFrZG93biI6ImFua3VuZnRzdmVyc3BhdHVuZyIsInN0YWNrZWQiOiJwZXJjZW50IiwiY29uZmlnIjp7ImRhdGFzZXQiOiJpc3QtZGF0ZW4tc2JiIiwib3B0aW9ucyI6e319fV0sInRpbWVzY2FsZSI6IiIsImRpc3BsYXlMZWdlbmQiOnRydWUsImFsaWduTW9udGgiOnRydWV9
    private final def FETCH_URLS = [
            // all records
            'https://data.sbb.ch/api/records/1.0/search/?dataset=ist-daten-sbb&facet=betreiber_id&facet=produkt_id&facet=linien_id&facet=linien_text&facet=verkehrsmittel_text&facet=faellt_aus_tf&facet=bpuic&facet=ankunftszeit&facet=an_prognose&facet=an_prognose_status&facet=ab_prognose_status&facet=ankunftsverspatung&facet=abfahrtsverspatung',
            // only delayed records
//            'https://data.sbb.ch/api/records/1.0/search/?dataset=ist-daten-sbb&q=ankunftsverspatung%3Dtrue&facet=betreiber_id&facet=produkt_id&facet=linien_id&facet=linien_text&facet=verkehrsmittel_text&facet=faellt_aus_tf&facet=bpuic&facet=ankunftszeit&facet=an_prognose&facet=an_prognose_status&facet=ab_prognose_status&facet=ankunftsverspatung&facet=abfahrtsverspatung'
    ]


    public void start() {
        def fetchedData = _fetchData()
        def formattedData = _formatData(fetchedData)
        def extractedDOI = _extractData(formattedData)
        print(formattedData)
    }

    private List<String> _fetchData() {

        List<String> fetchedData = []

        println "Start fetching data..."

        FETCH_URLS.each {

            def connection = new URL(it).openConnection() as HttpURLConnection

            // set headers
            connection.setRequestProperty('User-Agent', 'groovy-2.4.15')
            connection.setRequestProperty('Accept', 'application/json')

            // get the response code - automatically sends the request
            println "Response code: " + connection.responseCode
            String fetchedDataSet = connection.inputStream.text

            fetchedData.add(fetchedDataSet)
        }
        return fetchedData
    }

    private def _formatData(List<String> fetchedData) {

        List formattedData = []
        def jsonSlurper = new JsonSlurper()

        println "Start formatting data..."

        fetchedData.each {
            def dataMap = jsonSlurper.parseText(it as String)
            formattedData.add(dataMap)
        }
        return formattedData
    }

    private def _extractData(List<LazyMap> formattedData) {

        def date = new Date()
        def sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")

        def extractedData = []

        println "Start extracting data..."

        formattedData.each {

            def adjustedRecords = []
            it.records.each { record ->
                adjustedRecords.add([
                        "line": (record as LazyMap).fields["linien_id"],
                        "station": (record as LazyMap).fields["haltestellen_name"],
                        "vehicle": (record as LazyMap).fields["verkehrsmittel_text"],
                        "scheduled_arrival": (record as LazyMap).fields["ankunftszeit"],
                        "actual_arrival"   : (record as LazyMap).fields["an_prognose"],
                        "delay_at_arrival": (record as LazyMap).fields["ankunftsverspatung"],
                        "cancelled": (record as LazyMap).fields["faellt_aus_tf"],
                        "service_provider": (record as LazyMap).fields["betreiber_name"],
                        "geoPosition": (record as LazyMap).fields["geopos"],
                ])
            }

            def extractedDataSet = [
                    "fetchDate"      : sdf.format(date),
                    "numberOfRecords": it.nhits,
                    "records"        : adjustedRecords
            ]

            extractedData.add(extractedDataSet)
        }

        return extractedData
    }
}
