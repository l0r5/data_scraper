package ch.emobee.data_scraper.utils

import com.google.gson.Gson
import org.bson.Document

import java.util.logging.Logger

class DataFormatUtils {

    static def parseToMongoDoc(obj) {
        Gson gson = new Gson()
        def json = gson.toJson(obj)
        return Document.parse(json)
    }

    static void exportToFile(String data, String path, String fileName) {
        new File("${path}${fileName}").write(data)
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
