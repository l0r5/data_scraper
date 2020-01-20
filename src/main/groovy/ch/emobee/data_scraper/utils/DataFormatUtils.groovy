package ch.emobee.data_scraper.utils

import com.google.gson.Gson
import org.bson.Document

class DataFormatUtils {

    def static parseToMongoDoc(obj) {
        Gson gson = new Gson()
        def json = gson.toJson(obj)
        return Document.parse(json)
    }

    static void exportToFile(String data, String fileName, String path) {
        new File("${path}${fileName}").write(data)
        println "Data was exported under ${path}${fileName}"
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
