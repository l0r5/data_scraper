package ch.emobee.data_scraper.fetchers

import java.util.logging.Logger;

@Singleton
class TrainDataFetcherHandler {

    private final _logger = Logger.getLogger(TrainDataFetcherHandler.toString())

    void runCompleteFetchRoutine() {
        def fetchers = [new DBTrainDataFetcher().runCompleteRoutine(), new SBBTrainDataFetcher().runCompleteRoutine()]
        def threads = []

        fetchers.each {
            def thread = new Thread({it})
            threads << thread
        }

        threads.each { (it as Thread).start() }
        threads.each { (it as Thread).join() }
        _logger.info("Completed: runCompleteFetchRoutine().")
    }
}
