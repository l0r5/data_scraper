package ch.emobee.data_scraper

import ch.emobee.data_scraper.models.Operation
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.scheduling.annotation.EnableScheduling

@SpringBootApplication
@EnableScheduling
class DataScraperApplication {

    static void main(String[] args) {
        SpringApplication.run(DataScraperApplication, args)
//        new Operation().runOperation(Operation.OPERATION_TYPE_ALL_CALC)
//        new TrainDataFetcher().start()
    }
}
