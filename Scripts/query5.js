const { performance } = require('perf_hooks'); //to measure execution time
const MongoClient = require("mongodb").MongoClient; //library for mongoDB client
const { convertArrayToCSV } = require('convert-array-to-csv'); //library to convert two-dimensional arrays into csv files

fs = require('fs'); //FILE I/O

const url = "mongodb://localhost:27017/"; //url for connection to mongoDB server
const mongoClient = new MongoClient(url, {useNewUrlParser: true}); //creating instance of mongoDB client

async function resolveQuery() { //function to execute 5-th query
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
        let film = db.collection('film'); //"film" collection
        let filmList = await film.find({}).toArray(); //Array representation of "film" collection
        let filmActor = db.collection('film_actor'); //"film_category" collection
        let filmActorList = await filmActor.find({}).toArray(); //Array representation of "film_category" collection
        var startingActorID = 13; //starting actor from who we will count distance
        var queue = [startingActorID], used = [], dist = []; //queue=typical BFS queue, used=marked as visited actors, dist=distance to some actor 
        var graph = [], actorStarredFilm = []; //graph=adjacency list of actors, actorStarredFilm=if some actor starred in some film or not
        for (var i = 0; i < filmList.length; ++i){ //to calculate for each actor and each film did some actor starred in some film or not
            var curStarredFilm = [];  //to calculate for film being considered and each actor did some actor starred in this film or not
            for (var j = 0; j < actorList.length; ++j){
                curStarredFilm.push(false); //for now let no actor starred nowhere, because we'll calculate that later
            }
            actorStarredFilm.push(curStarredFilm); //adding starrings in current film to starrings in other films already considered
        }
        for (var i = 0; i < filmActorList.length; ++i){ //to calculate for each actor and each film did some actor starred in some film or not
            let filmID = filmActorList[i].row_to_json.film_id; //retrieving ID of film in film-actor entry being considered
            let actorID = filmActorList[i].row_to_json.actor_id; //retrieving ID of actor in film-actor entry being considered
            actorStarredFilm[filmID - 1][actorID - 1] = true; //adding info that actor with "actorID" starred in film with "filmID" to "actorStarredFilm"
        }
        for (var i = 0; i < actorList.length; ++i){ //traversing actors list
            used.push(false); //marking all vertices(actors) as unvisited first
            dist.push(100000); //making distance very huge to everyone
            let firstActorID = actorList[i].row_to_json.actor_id; //retrieving some actor's ID
            let coStarredRow = []; //array which will store actors co-starred with current actor
            for (var j = 0; j < actorList.length; ++j){ //traversing actors list 
                let secondActorID = actorList[j].row_to_json.actor_id; //retrieving some actor's ID
                let coStarred = false; //let first of all for first actor and second actor be not co-starred
                for (var k = 0; k < filmList.length; ++k){ //traversing films list
                    let filmID = filmList[k].row_to_json.film_id; //retrieving some film's ID
                    if (actorStarredFilm[filmID - 1][firstActorID - 1] && actorStarredFilm[filmID - 1][secondActorID - 1]){
                        coStarred = true;   //if both of actors starred in some film, then they co-starred, so we can put second actor to adjacency list of first actor
                        break;              //no need to traverse other films
                    }
                }
                if (coStarred == true){ //if both actors co-starred, add second actors to adjacency list of first actor
                    coStarredRow.push(secondActorID);
                }
            } 
            graph.push(coStarredRow); //add to graph adjacency list of first actor
        }
        used[startingActorID - 1] = true; //starting with our chosen actor, let actor be visited
        dist[startingActorID - 1] = 0;   //let distance to chosen actor be 0
        while(queue.length){ //typical BFS, until queue becomes empty
            let actorID = queue[0]; //let actorID be ID of first actor in the queue
            for (var i = 0; i < graph[actorID - 1].length; ++i){ //traversing adjacency list of current actor
                let nextActorID = graph[actorID - 1][i];  //retrieving ID of some actor who co-starred with current actor
                if (used[nextActorID - 1] == false){ //if next actor not visited
                    dist[nextActorID - 1] = dist[actorID - 1] + 1; //write distance to the next actor as distance to current actor + 1
                    queue.push(nextActorID); //push to our queue this next actor
                    used[nextActorID - 1] = true; //mark that next actor as visited
                }
            }
            queue.shift(); //delete current actor from queue because all transitions from him are already considered
        }
        var queryResult = [["ACTOR_ID", "DEGREE OF SEPARATION"]]; //inserting to the query result the topmost row of the table, columns' names
        for (var i = 0; i < actorList.length; ++i){ //traversing actors list
            let actorID = actorList[i].row_to_json.actor_id; //retrieving some actor's ID
            let newEntry = [actorID, dist[actorID - 1]]; //creating the pair of actorID and distance to the actor from starting actor
            queryResult.push(newEntry);                  //inserting this pair into query result
        }
        const csv = convertArrayToCSV(queryResult, {separator:";"}); //converting query result to csv
        fs.writeFileSync("query5.csv", csv); //writing resulting table to .csv file
    } catch (err) {
        console.log(err);
    } finally {
        client.close(); //closing connection
        var t1 = performance.now();  //finishing time
        console.log("Executed in " + (t1 - t0) + " milliseconds."); //execution time
    }
}

resolveQuery();