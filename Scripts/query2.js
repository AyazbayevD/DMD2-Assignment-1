const { performance } = require('perf_hooks'); //to measure execution time
const MongoClient = require("mongodb").MongoClient; //library for mongoDB client
const { convertArrayToCSV } = require('convert-array-to-csv'); //library to convert two-dimensional arrays into csv files

fs = require('fs'); //FILE I/O

const url = "mongodb://localhost:27017/"; //url for connection to mongoDB server
const mongoClient = new MongoClient(url, {useNewUrlParser: true}); //creating instance of mongoDB client

async function resolveQuery() {  //function to execute 2-nd query
    var t0 = performance.now(); //starting time
    const client = await MongoClient.connect(url, { useNewUrlParser: true }) //establishing connection with mongodb server
        .catch(err => { console.log(err); });
    if (!client) {
        return;
    }
    try {
        const db = client.db("dvdrental"); //our database
        let actor = db.collection('actor'); //"actor" collection
        let actorList = await actor.find({}).toArray(); //Array representation of "actor" collection
        let film = db.collection('film');  //"film" collection
        let filmList = await film.find({}).toArray();  //Array representation of "film" collection
        let filmActor = db.collection('film_actor');  //"film_actor" collection
        let filmActorList = await filmActor.find({}).toArray();  //Array representation of "film_actor" collection

        //coStarredTable=actual query result, actorStarredFilm=boolean table which says if some actor starred in some film or not, actorID=coStarredTable[0][0] in future
        var coStarredTable = [], actorStarredFilm = [], actorID = ["ACTOR_ID\\ACTOR_ID"]; 

        for (var i = 0; i < actorList.length; ++i){ //traversing actors list
            actorID.push(actorList[i].row_to_json.actor_id); //topmost row of the resulting query, actors' IDs  
        }
        for (var i = 0; i < filmList.length; ++i){  //to calculate for each actor and each film did some actor starred in some film or not
            let curStarredFilm = [];                  //to calculate for film being considered and each actor did some actor starred in this film or not
            for (var j = 0; j < actorList.length; ++j){
                curStarredFilm.push(false);             //for now let no actor starred nowhere, because we'll calculate that later
            }
            actorStarredFilm.push(curStarredFilm);      //adding starrings in current film to starrings in other films already considered
        }
        for (var i = 0; i < filmActorList.length; ++i){     //to calculate for each actor and each film did some actor starred in some film or not
            let filmID = filmActorList[i].row_to_json.film_id;  //retrieving ID of film in film-actor entry being considered
            let actorID = filmActorList[i].row_to_json.actor_id; //retrieving ID of actor in film-actor entry being considered
            actorStarredFilm[filmID - 1][actorID - 1] = true;   //adding info that actor with "actorID" starred in film with "filmID" to "actorStarredFilm"
        }
        coStarredTable.push(actorID); //pushing to query result topmost row
        for (var i = 0; i < actorList.length; ++i){  //traversing actors list
            let firstActorID = actorList[i].row_to_json.actor_id;  //retrieving ID of some actor
            let coStarredRow = [];  //to calculate actors that co starred with actor with "firstActorID"
            coStarredRow.push(firstActorID);  //leftmost column (actors' IDs)
            for (var j = 0; j < actorList.length; ++j){       //travering actors list 
                let secondActorID = actorList[j].row_to_json.actor_id;  //retrieving ID of another actor
                let coStarredCnt = 0;     //to count how many times actors with IDs "firstActorID" and "secondActorID" co starred
                for (var k = 0; k < filmList.length; ++k){ //traversing list of all films
                    let filmID = filmList[k].row_to_json.film_id;  //retrieving ID of some film
                    if (actorStarredFilm[filmID - 1][firstActorID - 1] && actorStarredFilm[filmID - 1][secondActorID - 1]){ //if both actors starred in that film
                        coStarredCnt++; //increase amount of co starrings of two actors being considered
                    }
                }
                coStarredRow.push(coStarredCnt); //add to row of actor "firstActorID" amount of co starrings with all other actors
            }
            coStarredTable.push(coStarredRow); //add calculated row to the query result
        }
        const csv = convertArrayToCSV(coStarredTable, {separator:";"}); //converting query result to csv
        fs.writeFileSync("query2.csv", csv); //writing resulting table to .csv file
    } catch (err) {
        console.log(err);
    } finally {
        client.close(); //closing connection
        var t1 = performance.now();  //finishing time
        console.log("Executed in " + (t1 - t0) + " milliseconds."); //execution time
    }
}

resolveQuery();
