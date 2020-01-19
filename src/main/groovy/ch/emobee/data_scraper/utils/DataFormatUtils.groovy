package ch.emobee.data_scraper.utils

import com.google.gson.Gson
import org.bson.Document

class DataFormatUtils {

    def static parseToMongoDoc(obj) {
        Gson gson = new Gson()
        def json = gson.toJson(obj)
        return Document.parse(json)
    }

}
