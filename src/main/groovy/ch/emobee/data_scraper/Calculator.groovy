package ch.emobee.data_scraper

import ch.emobee.data_scraper.services.MongoDBService
import ch.emobee.data_scraper.utils.DataFormatUtils

import java.text.SimpleDateFormat

class Calculator {

    // compare delays
    List dataSets = []
    List caclulatedDelays = []
    List caclulatedDelaysFlagTrue = []
    List caclulatedDelaysFlagFalse = []
    List delayFlag = []

    void start() {
        dataSets = MongoDBService.findAll()
        _compareDelays()
        def sortedDelays = _sortDelays(dataSets)
//        DataFormatUtils.exportToFile(DataFormatUtils.toCSV(sortedDelays), 'sorted-delay-name.csv', './output/processed/')
        def countedDelays = _countDelays(sortedDelays)
        countedDelays.each { k, v ->
            DataFormatUtils.exportToFile(DataFormatUtils.toCSV(v as Map), "$k-counted-delays.csv", './output/processed/')
        }

        // make prediction -> hier eventuell machine learning?? jemanden dazu fragen
// bzw. berechene verspätungs wahrscheinlcihkeit p für ein gewisses t

        println "Calculator finished."
    }

    private void _compareDelays() {

        dataSets.each {

            it["records"].each { record ->

                if (record["scheduled_arrival"] != null && record["actual_arrival"] != null) {

                    def dateScheduledArrival = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").parse(record["scheduled_arrival"] as String)
                    def dateActualArrival = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").parse(record["actual_arrival"] as String)
                    def difference = (dateScheduledArrival.getTime() - dateActualArrival.getTime()) / 1000
                    // norming to seconds

                    if (difference < 0) {
                        record["time_delayed"] = difference * -1
                        caclulatedDelays.add(record)
                        if (record["delay_at_arrival"] == "true") {
                            caclulatedDelaysFlagTrue.add(record)
                        } else {
                            caclulatedDelaysFlagFalse.add(record)
                        }
                    }
                }

                if (record["delay_at_arrival"] == "true") {
                    delayFlag.add(record)
                }
            }
        }


        println "Delays compared."
    }

    private static Map _sortDelays(dataSets) {

        Map collectedDelays = [:]
        Map collectedSortedDelayTimes = [:]

        // t -> wie viele Züge verspätet (in %) ab welcher Zeit
        dataSets.each { dataSet ->

            List timeDelayed = []

            dataSet["records"].each { record ->

                if (record["time_delayed"] != null) {
                    timeDelayed.add(record["time_delayed"])
                }
            }
            collectedDelays[dataSet['fetchDate']] = timeDelayed
        }


        collectedDelays.each { k, v ->

            Map sortedDelayTimes = [:]
            int remainingEntriesCounter = (v as List).size()

            for (int i = 1; remainingEntriesCounter > 0; i++) {

                def matches

                // make 120 minutes the upper bound where all results are aggregated
                if (i >= 120) {
                    matches = (v as List).findAll { element ->
                        element >= i * 60
                    }
                    remainingEntriesCounter = 0
                    // else count hits for every minute of delay
                } else {
                    matches = (v as List).findAll { element ->
                        element <= i * 60 && element > (i - 1) * 60
                    }
                    remainingEntriesCounter = remainingEntriesCounter - matches.size()
                }

                sortedDelayTimes[i] = matches
                println("Counted ${matches.size()} delays with a delay of $i Minutes. ${remainingEntriesCounter} entries remaining to be sorted.")
            }

            collectedSortedDelayTimes[k] = sortedDelayTimes
        }

        println "Calculated delays."
        return collectedSortedDelayTimes
    }

    private static Map _countDelays(dataSets) {

        Map collectedCountedDelays = [:]
        dataSets.each { date, values ->
            Map countedDelays = [:]
            values.each { k, v ->
                countedDelays[k] = (v as ArrayList).size()
            }
            collectedCountedDelays[date] = countedDelays
        }
        return collectedCountedDelays
    }
}