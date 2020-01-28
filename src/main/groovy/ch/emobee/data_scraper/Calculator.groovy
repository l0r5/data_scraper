package ch.emobee.data_scraper

import ch.emobee.data_scraper.models.Operation
import ch.emobee.data_scraper.services.MongoDBService
import ch.emobee.data_scraper.utils.DataFormatUtils
import groovy.json.JsonOutput

import java.text.SimpleDateFormat
import java.util.logging.Logger

class Calculator {

    private final logger = Logger.getLogger(Calculator.toString())

    String calc(operationType) {
        String result = ''
        switch (operationType) {
            case Operation.OPERATION_TYPE_ALL_CALC:
                result =  _runAllCalculations()
                break
            default:
                logger.warning("Invalid operationType entered.")
        }
        return result
    }

    private String _runAllCalculations() {

        List dataSets = MongoDBService.findAll()

        def delaySets = _createDelaySets(dataSets)
        def countedDelaySets = _countDelays(delaySets)
        def mergedCountedDelaySet = _mergeCountedDelays(countedDelaySets)
        exportCountedDelayFiles(countedDelaySets)
        exportMergedCountedDelayFiles(mergedCountedDelaySet)

        // make prediction -> hier eventuell machine learning?? jemanden dazu fragen
        // bzw. berechene verspätungs wahrscheinlcihkeit p für ein gewisses t
        logger.info("Calculator finished calculation: AllCalculations.")
        return JsonOutput.toJson(mergedCountedDelaySet)
    }

    private List _calculateDelayTimes(dataSets) {
        // compare delays
        List caclulatedDelays = []
        List caclulatedDelaysFlagTrue = []
        List caclulatedDelaysFlagFalse = []
        List delayFlagTrue = []

        dataSets.each {
            it["records"].each { record ->
                if (record["scheduled_arrival"] != null && record["actual_arrival"] != null) {
                    def dateScheduledArrival = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").parse(record["scheduled_arrival"] as String)
                    def dateActualArrival = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").parse(record["actual_arrival"] as String)
                    def difference = (dateScheduledArrival.getTime() - dateActualArrival.getTime()) / 1000
                    // norming to seconds
                    if (difference < 0) {
                        record["delay_time"] = difference * -1
                        caclulatedDelays.add(record)
                        if (record["delay_at_arrival"] == "true") {
                            caclulatedDelaysFlagTrue.add(record)
                        } else {
                            caclulatedDelaysFlagFalse.add(record)
                        }
                    }
                }
                if (record["delay_at_arrival"] == "true") {
                    delayFlagTrue.add(record)
                }
            }
            int numberAllFlagTrue = delayFlagTrue.size()
            int numberCalcFlagTrue = caclulatedDelaysFlagTrue.size()
            int numberCalcFlagFalse = caclulatedDelaysFlagFalse.size()
            logger.info ("Entries with fetch date: ${it["fetchDate"]}")
            logger.info("The total number of entries is: ${(it["numberOfRecords"])}")
            logger.info("The number of entries with the flag delay_at_arrival = true: $numberAllFlagTrue.")
            logger.info("The number of entries with calc delay times and flag delay_at_arrival = true: $numberCalcFlagTrue.")
            logger.info("The number of entries whith calc delay times and flag delay_at_arrival = false: $numberCalcFlagFalse.")
        }
        logger.info("Delay times were calculated.")
        return dataSets
    }

    private Map _createDelaySets(dataSets) {

        Map collectedDelays = [:]
        Map collectedSortedDelayTimes = [:]

        def setsWithDelayTimes = _calculateDelayTimes(dataSets)

        setsWithDelayTimes.each { dataSet ->
            List timeDelayed = []
            dataSet["records"].each { record ->
                if (record["delay_time"] != null) {
                    timeDelayed.add(record["delay_time"])
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
            }
            collectedSortedDelayTimes[k] = sortedDelayTimes
        }
        logger.info( "Created delay sets.")
        return collectedSortedDelayTimes
    }

    private Map _countDelays(dataSets) {
        Map collectedCountedDelays = [:]
        dataSets.each { date, values ->
            Map countedDelays = [:]
            values.each { k, v ->
                countedDelays["$k"] = (v as ArrayList).size()
            }
            collectedCountedDelays[date] = countedDelays
        }
        logger.info("Counted delays.")
        return collectedCountedDelays
    }

    private Map _mergeCountedDelays(dataSets) {

        Map mergedCountedDelays = [:]
        dataSets.each { date, values ->
            if (mergedCountedDelays['dates'] == null || mergedCountedDelays['total-delay-times'] == null) {
                mergedCountedDelays['dates'] = [date]
                mergedCountedDelays['total-delay-times'] = values
            } else {
                Map mergedValues = mergedCountedDelays['total-delay-times'] as Map
                values.each { k, v ->
                    if (mergedValues[k] == null) {
                        mergedValues[k] = v
                    } else {
                        mergedValues[k] += v
                    }
                }
                mergedCountedDelays['total-delay-times'] = mergedValues as Map
                (mergedCountedDelays['dates'] as List).add(date as String)
            }
        }
        logger.info("Merged counted delays.")
        return mergedCountedDelays
    }

    private static void exportCountedDelayFiles(countedDelaySets) {
        countedDelaySets.each { k, v ->
            String fileName = "$k-counted-delays.csv"
            new DataFormatUtils().exportToFile(DataFormatUtils.toCSV(v as Map), "$fileName", './output/processed/')
        }
    }

    private static void exportMergedCountedDelayFiles(mergedCountedDelaySet) {
        String fileName = "${(mergedCountedDelaySet["dates"] as List).get(0)}-to-${(mergedCountedDelaySet["dates"] as List).get((mergedCountedDelaySet["dates"] as List).size() - 1)}-total-counted-delays.csv"
        String fileNameCurrent = "total-counted-delays.json"
        new DataFormatUtils().exportToFile(DataFormatUtils.toCSV(mergedCountedDelaySet["total-delay-times"] as Map), "$fileName", './output/processed/')
        new DataFormatUtils().exportToFile(JsonOutput.toJson(mergedCountedDelaySet), "$fileNameCurrent", './output/current/')
    }

}