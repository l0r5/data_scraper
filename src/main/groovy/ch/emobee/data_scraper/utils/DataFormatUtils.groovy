package ch.emobee.data_scraper.utils

import com.google.gson.Gson
import org.bson.Document

import java.util.logging.Logger

class DataFormatUtils {

    private final logger = Logger.getLogger(DataFormatUtils.toString())

    def static parseToMongoDoc(obj) {
        Gson gson = new Gson()
        def json = gson.toJson(obj)
        return Document.parse(json)
    }

    void exportToFile(String data, String fileName, String path) {
        new File("${path}${fileName}").write(data)
        logger.info("Created file $fileName under $path")
    }

    static String toCSV(Map input) {
        String output = ''
        input.each { k, v ->
            output = output + "$k,$v\n"
            output.replaceAll("\\s", "")
        }
        return output
    }
}
