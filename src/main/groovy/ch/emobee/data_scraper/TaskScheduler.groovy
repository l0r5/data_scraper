package ch.emobee.data_scraper

import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

import java.text.SimpleDateFormat

@Component
class AppTaskScheduler {

    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    @Scheduled(cron="0 15 22 ? * *")
    static void startTrainDataJob() {
        println "Job triggered at: ${sdf.format(new Date())}"
        new TrainDataFetcher().start()
    }
}
