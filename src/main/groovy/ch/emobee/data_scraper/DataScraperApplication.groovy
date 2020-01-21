package ch.emobee.data_scraper

import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.scheduling.annotation.EnableScheduling

@SpringBootApplication
@EnableScheduling
class DataScraperApplication {

	static void main(String[] args) {
		SpringApplication.run(DataScraperApplication, args)
//		new TrainDataFetcher().start()
//		new Calculator().calculate(C)
	}

}
