const { performance } = require('perf_hooks'); //to measure execution time
const MongoClient = require("mongodb").MongoClient; //library for mongoDB client

fs = require('fs');  //FILE I/O

const url = "mongodb://localhost:27017/"; //url for connection to mongoDB server
const mongoClient = new MongoClient(url, {useNewUrlParser: true}); //creating instance of mongoDB client

async function resolveQuery() { //function to execute 4-th query
    var t0 = performance.now(); //starting time
    const client = await MongoClient.connect(url, { useNewUrlParser: true }) //establishing connection with mongodb server
        .catch(err => { console.log(err); });
    if (!client) {
        return;
    }
    try {
        const db = client.db("dvdrental"); //our database
        const customer = db.collection("customer"); //"customer" collection
        const customerList = await customer.find({}).toArray(); //Array representation of "customer" collection
        const rental = db.collection("rental"); //"rental" collection
        const rentalList = await rental.find({}).toArray(); //Array representation of "rental" collection
        const film = db.collection("film"); //"film" collection
        const filmList = await film.find({}).toArray(); //Array representation of "film" collection
        const ourCustomerID = 433; //defining the customer for whom we will recommend movies

        //customerFilm=list of rented films for each customer, customerFilmTable=if some customer rented some movie or not,
        //metric=if some customer have similar set of movies with our customer, where x% of films match, then recommendation metric of that film will be floor(x/10)
        var customerFilm = [], customerFilmTable = [], metric = [];

        for (var i = 0; i < filmList.length; ++i){ //traversing films list
            metric.push(0); //initializing "metric" array
        }
        for (var i = 0; i < customerList.length; ++i){ //traversing customers list
            customerFilm.push([]); //initializing "customerFilm" array
            let curCustomerFilm = []; //initializing some row of "customerFilmTable" array
            for (var j = 0; j < filmList.length; ++j){
                curCustomerFilm.push(false);//initializing some row of "customerFilmTable" array
            }
            customerFilmTable.push(curCustomerFilm);//initializing some row of "customerFilmTable" array
        }
        for (var i = 0; i < rentalList.length; ++i){ //traversing rentals list
            let customerID = rentalList[i].row_to_json.customer_id; //retrieving some customer's ID who rented some film
            let filmID = rentalList[i].row_to_json.inventory.film.film_id; //retrieving some film which was rented by this customer
            customerFilm[customerID - 1].push(filmID); //adding this film to list of rented films of correspondent customer
            customerFilmTable[customerID - 1][filmID - 1] = true; //affirm that some customer watched some film
        }
        for (var i = 0; i < customerList.length; ++i){ //traversing customers list
            let customerID = customerList[i].row_to_json.customer_id; //retrieving ID of some customer
            if (customerID == ourCustomerID) continue; //if we found our customer, useless to consider it, because metrics will not change
            let matchCnt = 0; //how many rented films matched of some customer with our customer
            
            for (var j = 0; j < customerFilm[customerID - 1].length; ++j){ //traversing films list of some customer
                let filmID = customerFilm[customerID - 1][j]; //retrieving film's ID of that customer
                if (customerFilmTable[ourCustomerID - 1][filmID - 1]) matchCnt++; //if our customer also rented that film, incrementing counter
            }
            let matchPercentage = matchCnt * 100 / customerFilm[customerID - 1].length; //calculating matching percentage for current customer
            let metricCalc = Math.floor(matchPercentage / 10); //calculating the metric of films watched by current customer but not watched by our customer
            for (var j = 0; j < customerFilm[customerID - 1].length; ++j){  //traversing films list of current customer
                let filmID = customerFilm[customerID - 1][j]; //retrieving some film's ID from the list
                if (customerFilmTable[ourCustomerID - 1][filmID - 1] == false){ //if our customer did not rent this film yet, so we update metric for the film
                    if (metric[filmID - 1] < metricCalc){                       //if metric for the film was less before this
                        metric[filmID - 1] = metricCalc;                        //so we find maximum metric for some film among all customers
                    }
                }
            }
        }
        var queryResult = []; //query result array which will contain films to be recommended
        for (var i = 10; i >= 0; --i){  //starting from highest possible metric
            for (var j = 0; j < filmList.length; ++j){ //for each metric traversing films list
                let filmID = filmList[j].row_to_json.film_id; //retrieving some film's ID 
                if (metric[filmID - 1] == i){      //if metric of that film matches the metric we are considering,
                    queryResult.push(filmList[j]); //then we add this film to recommendations
                }
            }
            if (queryResult.length > 0){ //if we did not find any film cur current metric, we continue with less metric,
                break;                   //otherwise, we have something to recommend, and we recommend it
            }
        }
        fs.writeFileSync("query4.json", JSON.stringify(queryResult, null, 4)); //writing result to .json file
    } catch (err) {
        console.log(err);
    } finally {
        client.close(); //closing the connection
        var t1 = performance.now();  //finishing time
        console.log("Executed in " + (t1 - t0) + " milliseconds."); //execution time
    }
}

resolveQuery();
