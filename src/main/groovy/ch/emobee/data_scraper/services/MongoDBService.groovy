package ch.emobee.data_scraper.services

import ch.emobee.data_scraper.utils.DataFormatUtils
import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import org.bson.Document

class MongoDBService {

    def static save(obj) {
        MongoClient mongoClient = MongoClients.create()
        MongoDatabase db = mongoClient.getDatabase("train-delays")
        MongoCollection<Document> collection = db.getCollection("extract-scheduled-actual-arrival")
        Document doc = DataFormatUtils.parseToMongoDoc(obj)
        collection.insertOne(doc)
    }


}
