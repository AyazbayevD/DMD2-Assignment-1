const { performance } = require('perf_hooks'); //to measure execution time
const MongoClient = require("mongodb").MongoClient; //library for mongoDB client
fs = require('fs'); //FILE I/O

const url = "mongodb://localhost:27017/"; //url for connection to mongoDB server
const mongoClient = new MongoClient(url, {useNewUrlParser: true}); //creating instance of mongoDB client

var getYear = function(date){ //function which retrieves year from date which is given in string format
    var year = 0; //year to be calculated
    for (var i = 0; i < date.length; ++i){ //traversing given string                 
        if (date[i] == '-') break; //if we found end of year part then we're done
        year *= 10; //these two procedures serve as adding sone digit to the end of some number
        year += date[i].codePointAt(0) - 48;
    }
    return year; //calculated year
}   

async function resolveQuery() { //function to execute 1-st query
    var t0 = performance.now(); //starting time
    const client = await MongoClient.connect(url, { useNewUrlParser: true }) //establishing connection with mongodb server
        .catch(err => { console.log(err); });
    if (!client) {
        return;
    }
    try {
        const db = client.db("dvdrental"); //our database
        let queryResult = []; //resulting query which is actually json collection
        let rental = db.collection('rental'); //"rental" collection
        let rentalList = await rental.find({}).toArray();  //Array representation of "rental" collection
        let customer = db.collection('customer');  //"customer" collection
        let customerList = await customer.find({}).toArray(); //Array representation of "customer" collection
        let filmCategory = db.collection('film_category');  //"film_category" collection
        let filmCategoryList = await filmCategory.find({}).toArray(); //Array representation of "film_category" collection
        var curYear = 0; //current year to be calculated
        for (var i = 0; i < rentalList.length; ++i){ //traversing all rentals
            var rentalDate = rentalList[i].row_to_json.rental_date;   //getting date of some rental
            var rentalYear = getYear(rentalDate);     //calculating year of some rental
            if (rentalYear > curYear) curYear = rentalYear;   //this is to calculate maximum rental year = will be our current year
        }
        for (var i = 0; i < customerList.length; ++i){  //traversing all customers
            var diffCategory = 0;      //this variable is to find out did customer rent films of at least two different categories during current year
            var customerID = customerList[i].row_to_json.customer_id;  //retrieving some customer's ID
            for (var j = 0; j < rentalList.length; ++j){   //traversing all rentals
                var rentalDate = rentalList[j].row_to_json.rental_date;   //retrieving some rental's date
                var rentalYear = getYear(rentalDate);      //calculating year of some rental
                if (rentalYear < curYear || rentalList[j].row_to_json.customer_id != customerID){ //if year is not current year,
                    continue;                                                                     //or not customer who is being considered rented the film, omit this rental
                }  
                var rentedFilmID = rentalList[j].row_to_json.inventory.film.film_id; //retrieving ID of the film which is rented by current customer who is being considered
                for (var k = 0; k < filmCategoryList.length; ++k){ //traversing table with mapping of films to their categories
                    if (filmCategoryList[k].row_to_json.film_id == rentedFilmID){  //if we found in this map film from rental which is being considered
                        if (diffCategory == 0){                                             //if we did not find film in the map yet
                            diffCategory = filmCategoryList[k].row_to_json.category_id;
                        }
                        else if (diffCategory != filmCategoryList[k].row_to_json.category_id){  //if we already found film in the map, and checking
                            diffCategory = -1;                                                  //if previously found film's category differs from the new one,
                            break;                                                              //then we add to result the customer who is being considered,
                        }                                                                       //because we already found two different categories of rented films
                    }
                }
                if (diffCategory == -1){ //if we found at least 2 different categories, no need to consider further
                    break;
                }
            }
            if (diffCategory == -1){ //if we found at least 2 different categories, we include customer to the result
                queryResult.push(customerList[i]);
            }
        }
        fs.writeFileSync("query1.json", JSON.stringify(queryResult, null, 4)); //writing to json file the found customers
    } catch (err) {
        console.log(err);
    } finally {
        client.close();         //closing connection
        var t1 = performance.now();  //finishing time
        console.log("Executed in " + (t1 - t0) + " milliseconds."); //execution time
    }
}

resolveQuery();