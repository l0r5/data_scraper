package ch.emobee.data_scraper.services


import ch.emobee.data_scraper.models.Operation
import groovy.json.JsonSlurper
import groovy.json.internal.LazyMap
import org.springframework.web.bind.annotation.CrossOrigin
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController

import java.util.logging.Logger;

@RestController
public class RestAPIService {

    private final logger = Logger.getLogger(RestAPIService.toString())
    private Operation operation = new Operation()

    @CrossOrigin
    @GetMapping("/api/calc-total-counted-delays")
    String calcTotalCountedDelays() {
        logger.info("GET  /api/calc-total-counted-delays")
        operation.runOperation(Operation.OPERATION_TYPE_ALL_CALC)
        logger.info("GET  /api/calc-total-counted-delays -> result:")
        logger.info("${operation.getResult()}")
        return operation.getResult()
    }

    @CrossOrigin
    @GetMapping("/api/get-total-counted-delays")
    Map getTotalCountedDelays() {
        logger.info("GET  /api/get-total-counted-delays")

        JsonSlurper jsonSlurper = new JsonSlurper()
        LazyMap data = []
        try {
            data = jsonSlurper.parse(new File('./output/current/total-counted-delays.json')) as LazyMap
        } catch (Exception e) {
            print("Error during reading file: ${e.printStackTrace()}")
        }
        return data
    }
}
