const { performance } = require('perf_hooks'); //to measure execution time
const fs = require('fs');   //file I/O

const { Client } = require('pg'); //postgreSQL client
const pgClient = new Client({     //PSQL client parameters
    user: "postgres",           
    password: "postgres",
    host: "localhost",
    port: 5432,
    database: "dvdrental",
});

//connections between tables: (even indexes: table name, odd indexes: to which tables table at index - 1 has foreign keys)
var rel = ["category", [], "film", ["language"], "language", [], "actor", [], "inventory", ["film", "store"], "rental", ["inventory", "customer", "staff"],
    "payment", ["customer", "staff", "rental"], "staff", ["address", "store"], "customer", ["store", "address"], "address", ["city"], "city", ["country"], 
    "country", [], "store", ["address", "staff"]];

//function for composing sql queries, "curRel"=current table, "prevRel"=table from which we came by foreign key to "curRel"
//function is recursive, composing nested SQL queries, which return JSON text and put into JSON files
var composeQuery = function(curRel, prevRel){  
    var i, curRelIndex;   //even curRelIndex = index of "curRel" in the "rel" array
    for (i = 0; i < rel.length; i += 2){ //calculating curRelIndex
        if (curRel.localeCompare(rel[i]) == 0){
            curRelIndex = i;
            break;
        }
    }

    //if we do not have foreign keys, we stop nesting queries at current point
    if (rel[curRelIndex + 1].length == 0 || (rel[curRelIndex + 1].length == 1 && rel[curRelIndex + 1][0].localeCompare(prevRel) == 0)){
        return " from " + curRel + " where " + curRel + "." + curRel + "_id = " + prevRel + "." + curRel + "_id\n";
    }

    var curSubQuery = ""; //the part of query which will supplement the query in the less deep level of recursion
    for (i = 0; i < rel[curRelIndex + 1].length; i++){ //searching all foreign keys to other tables
        if (prevRel.localeCompare(rel[curRelIndex + 1][i]) == 0){ //we do not consider foreign keys to "prevRel", because we will enter in infinite cycle
            continue;                                             //the only case is with "store" and "staff" tables, but still
        }

        //composing nested query by retrieving query part from deeper level of recursion
        curSubQuery += ",\n(\n" + "select row_to_json(" + rel[curRelIndex + 1][i] + "1) from\n(\nselect * " + 
                        composeQuery(rel[curRelIndex + 1][i], curRel) + "\n)" + rel[curRelIndex + 1][i] + "1\n) as " + rel[curRelIndex + 1][i] + " ";

        if (i == rel[curRelIndex + 1].length - 1 || rel[curRelIndex + 1][i + 1].localeCompare(prevRel) == 0){ //if no other foreign keys we finish calling recursion
            curSubQuery += "from " + curRel + "\n";
        }
    }
    if ((curRel.localeCompare("staff") == 0 && prevRel.localeCompare("store") == 0) || //issues with names (not "store.staff_id", but "store.manager_staff_id") columns
    (curRel.localeCompare("store") == 0 && prevRel.localeCompare("staff") == 0)){      //are present in the "store" table
        curSubQuery += "where staff.staff_id = store.manager_staff_id\n";//finishing composing query part in current recursion call
    }
    else {
        curSubQuery += "where " + curRel + "." + curRel + "_id = " + prevRel + "." + curRel + "_id\n";//finishing composing query part in current recursion call
    }
    return curSubQuery; //giving composed part of a query back to the less deep level of recursion
}

generateQueries();//calling function to generate and execute SQL queries
async function generateQueries(){ //function to generate and execute SQL queries
    var t0 = performance.now(); //starting time
    await pgClient.connect(); //connecting to PSQL Server
    var i; 
    for (i = 0; i < rel.length; i += 2){ //traversing "rel" and generating SQL query for every table

        //composing final query with the help of recursion
        var curSqlQuery = "select row_to_json(" + rel[i] + "1) from\n(\nselect * " + composeQuery(rel[i], rel[i]) + "\n)" + rel[i] + "1;";

        const res = await pgClient.query(curSqlQuery); //querying PSQL Server
        fs.writeFileSync(rel[i] + ".json", JSON.stringify(res.rows, null, 4)); //writing results of queries for each table into correspondent JSON files
    }
    var res; //another kind of query for "film_category" table
    res = await pgClient.query(` 
        select row_to_json(film_category) from 
        (
            select *,
            (
                select row_to_json(category) from category where category.category_id = film_category.category_id
            ) as category, 
            (
                select row_to_json(film) from 
                (
                    select *,
                    (
                        select row_to_json(language) from language where film.language_id = language.language_id
                    ) as language from film  
                    where film_category.film_id = film.film_id
                ) film
            ) as film from film_category
        ) 
        film_category;  
    `)
    fs.writeFileSync("film_category.json", JSON.stringify(res.rows, null, 4));
    
    //another kind of query for "film_actor" table
    res = await pgClient.query(`
        select row_to_json(film_actor) from 
        (
            select *,
            (
                select row_to_json(actor) from actor where actor.actor_id = film_actor.actor_id
            ) as actor, 
            (
                select row_to_json(film) from 
                (
                    select *,
                    (
                        select row_to_json(language) from language where film.language_id = language.language_id
                    ) as language from film  
                    where film_actor.film_id = film.film_id
                ) film
            ) as film from film_actor
        ) 
        film_actor;  
    `)
    fs.writeFileSync("film_actor.json", JSON.stringify(res.rows, null, 4));
    await pgClient.end(); //closing connection
    var t1 = performance.now();  //finishing time
    console.log("Executed in " + (t1 - t0) + " milliseconds."); //execution time
}
