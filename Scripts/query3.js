const { performance } = require('perf_hooks'); //to measure execution time
const MongoClient = require("mongodb").MongoClient; //library for mongoDB client
const { convertArrayToCSV } = require('convert-array-to-csv'); //library to convert two-dimensional arrays into csv files

fs = require('fs');  //FILE I/O

const url = "mongodb://localhost:27017/"; //url for connection to mongoDB server
const mongoClient = new MongoClient(url, {useNewUrlParser: true});  //creating instance of mongoDB client

async function resolveQuery() { //function to execute 3-rd query
    var t0 = performance.now(); //starting time
    const client = await MongoClient.connect(url, { useNewUrlParser: true }) //establishing connection with mongodb server
        .catch(err => { console.log(err); });
    if (!client) {
        return;
    }
    try {
        const db = client.db("dvdrental"); //our database
        const rental = db.collection("rental"); //"rental" collection
        const rentalList = await rental.find({}).toArray(); //Array representation of "rental" collection
        const film = db.collection("film");  //"film" collection
        const filmList = await film.find({}).toArray(); //Array representation of "film" collection
        const filmCategory = db.collection("film_category"); //"film_category" collection
        const filmCategoryList = await filmCategory.find({}).toArray(); //Array representation of "film_category" collection

        //rentedCnt=how many times some film was rented, categoryFilm=what is the category of some film, queryResult=query result table
        var rentedCnt = [], categoryFilm = [], queryResult = []; 

        for (var i = 0; i < filmList.length; ++i){ //to initialize arrays "rentedCnt" and "categoryFilm"
            categoryFilm.push(0);
            rentedCnt.push(0);
        }
        for (var i = 0; i < rentalList.length; ++i){ //traversing rentals list
            let filmID = rentalList[i].row_to_json.inventory.film.film_id; //retrieving ID of some film
            rentedCnt[filmID - 1]++; //increasing the number of times this film was rented
        }
        for (var i = 0; i < filmCategoryList.length; ++i){ //traversing list of mapping from films to their categories
            let filmID = filmCategoryList[i].row_to_json.film_id; //retrieving some film's ID
            let categoryID = filmCategoryList[i].row_to_json.category_id; //retrieving that film's category
            categoryFilm[filmID - 1] = categoryID; //assigning this category to the film with "filmID" ID, to access category by film in O(1) time in future
        }
        queryResult.push(["FILM_ID", "CATEGORY_ID", "RENTED_COUNT"]); //topmost row of the query result table, columns' names
        for (var i = 0; i < filmList.length; ++i){  //traversing films list 
            let filmID = filmList[i].row_to_json.film_id; //retrieving ID of some film
            let curFilmInfo = [];  //initializing array for some row of query result table
            curFilmInfo.push(filmID); //adding filmID
            curFilmInfo.push(categoryFilm[filmID - 1]); //adding category of film
            curFilmInfo.push(rentedCnt[filmID - 1]); //adding number of rentals of film
            queryResult.push(curFilmInfo); //adding calculated row for film to the query result table
        }
        const csv = convertArrayToCSV(queryResult, {separator:";"}); //converting query result to csv
        fs.writeFileSync("query3.csv", csv); //writing resulting table to .csv file
    } catch (err) {
        console.log(err);
    } finally {
        client.close(); //closing connection
        var t1 = performance.now();  //finishing time
        console.log("Executed in " + (t1 - t0) + " milliseconds."); //execution time
    }
}

resolveQuery();
