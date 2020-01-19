package ch.emobee.data_scraper

import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication

@SpringBootApplication
class DataScraperApplication {

	static void main(String[] args) {
		SpringApplication.run(DataScraperApplication, args)
		new TrainDataFetcher().start();
	}

}