package ch.emobee.data_scraper.services


import ch.emobee.data_scraper.models.Operation
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController;

import java.util.logging.Logger;

@RestController
public class RestAPIService {

    private final logger = Logger.getLogger(RestAPIService.toString())
    private Operation operation  = new Operation()

    @RequestMapping("/api/calc-total-counted-delays")
    String result(@RequestParam(value = "result", defaultValue = "") result) {
        logger.info("GET  /api/calc-total-counted-delays")
        operation.start(Operation.OPERATION_TYPE_ALL_CALC)
        logger.info("GET  /api/calc-total-counted-delays -> result:")
        logger.info("${operation.getResult()}")
        return operation.getResult()
    }
//
//    @RequestMapping(value = "/api/lobby/addOnlineUser", method = RequestMethod.POST)
//    @ResponseBody
//    List<String> addOnlineUser(String user) {
//        lobby.addOnlineUser(user)
//        logger.info("Added user $user, Lobby -> usersOnline: ${lobby.usersOnline.toString()}")
//        return lobby.usersOnline
//    }
//
//    @RequestMapping(value = "/api/lobby/removeOnlineUser", method = RequestMethod.POST)
//    @ResponseBody
//    List<String> removeOnlineUser(String user) {
//        lobby.removeOnlineUser(user)
//        logger.info( "Removed user $user, Lobby -> usersOnline: ${lobby.usersOnline.toString()}")
//        return lobby.usersOnline
//    }
}
