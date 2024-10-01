// import { ConfluencePagesLoader } from "langchain/document_loaders/web/confluence";
// var cpl = require("langchain/document_loaders/web/confluence");
var express = require("express");
var bodyParser = require("body-parser");
var cors = require("cors");
// var multer = require("multer");
var app = express();
var path = require("path");
var shell = require("shelljs");
var fs = require("fs");
var timeout = require("connect-timeout");
var pyshell = require("python-shell");
const { response } = require("express");
// const db = require("./db.js");
// var ISO_date = new Date().toISOString().replace(/T/, "-").replace(/\..+/, "");

function haltOnTimedout(req, res, next) {
	if (!req.timedout) next();
}

function initialize() {
	try {
		options = { args: [] };
		pyshell.PythonShell.run(
			path.join(__dirname + "/initialize.py"),
			options,
			function (err, results) {
				if (err) {
					console.log(err);
				} else {
					console.log("Vector DB created successfully!");
				}
			}
		);
	} catch (e) {
		console.error("Error initializing db: ", e);
	}
}

app.use(timeout(25000000));
app.use(haltOnTimedout);
app.use(bodyParser.json());
app.use(cors());
app.locals.basedir = __dirname;
app.use(express.static(__dirname));

app.get("test", (req, res) => {
	res.status(200).send("TEST SUCCESSFULL");
	console.log("TEST SUCCESSFULL");
});

app.post("/chatNew", (req, res) => {
	req.socket.setTimeout(10 * 60 * 1000);

	try {
		query = JSON.stringify(req.body.query);
		chat_history = JSON.stringify(req.body.chat_history);
		conversation_id = JSON.stringify(req.body.conversation_id);
		qa_id = JSON.stringify(req.body.qa_id);
		user_id = JSON.stringify(req.body.user_id);

		options = {
			args: [query, chat_history, conversation_id, qa_id, user_id],
		};
		console.log("/chatNew - QUERY: " + query);
		console.log("/chatNew - CHAT_HISTORY: " + chat_history);
		console.log("/chatNew - conversation_id: " + conversation_id);
		console.log("/chatNew - qa_id: " + qa_id);
		console.log("/chatNew - user_id: " + user_id);
		pyshell.PythonShell.run(
			path.join(__dirname + "/chat.py"),
			options,
			function (err, results) {
				if (err) {
					console.log(err);
					res.status(500).send(
						"Sorry, there was a system error. Please refresh the page and try again.<br/>Error 500: " +
							String(err)
					);
					console.log("Error 500 console log.");
					return;
				} else {
					try {
						console.log(results);
						res.status(200).send(
							JSON.parse(results[results.length - 1])
						);
						console.log(req.timedout);
						console.log("Chat Completed.");
					} catch (err) {
						console.log(err);
						res.status(500).send(
							"Sorry, there was a system error. Please refresh the page and try again.<br/>Error 500: " +
								String(err)
						);
						console.log("Error 500 console log.");
						return;
					}
				}
			}
		);
	} catch (err) {
		console.log("Catch", err);
		res.status(500).send("Server Error. Please refresh and try again.");
		console.log("500 Catch Error");
	}

	return;
});

app.post("/castScore", (req, res) => {
	req.socket.setTimeout(10 * 60 * 1000);

	try {
		human_score = JSON.stringify(req.body.human_score);
		qa_id = JSON.stringify(req.body.qa_id);
		user_id = JSON.stringify(req.body.user_id);

		options = {
			args: [human_score, qa_id, user_id],
		};
		console.log("/castScore - human_score: " + human_score);
		console.log("/castScore - qa_id: " + qa_id);
		console.log("/castScore - user_id: " + user_id);
		pyshell.PythonShell.run(
			path.join(__dirname + "/cast_score.py"),
			options,
			function (err, results) {
				if (err) {
					console.log(err);
					res.status(500).send(
						"Sorry, there was a system error. Please refresh the page and try again.<br/>Error 500: " +
							String(err)
					);
					console.log("Error 500 console log.");
					return;
				} else {
					try {
						console.log(results);
						res.status(200).send(
							String(results[results.length - 1])
						);
						console.log(req.timedout);
						console.log("Casting Score Completed.");
					} catch (err) {
						console.log(err);
						res.status(500).send(
							"Sorry, there was a system error. Please refresh the page and try again.<br/>Error 500: " +
								String(err)
						);
						console.log("Error 500 console log.");
						return;
					}
				}
			}
		);
	} catch (err) {
		console.log("Catch", err);
		res.status(500).send("Server Error. Please refresh and try again.");
		console.log("500 Catch Error");
	}

	return;
});

app.post("/refreshData", (req, res) => {
	req.socket.setTimeout(10 * 60 * 1000);

	try {
		options = {};
		pyshell.PythonShell.run(
			path.join(__dirname + "/initialize.py"),
			options,
			function (err, results) {
				if (err) {
					console.log(err);
					res.status(500).send(
						"Sorry, there was a system error. Please refresh the page and try again.<br/>Error 500: " +
							String(err)
					);
					console.log("Error 500 console log.");
					return;
				} else {
					try {
						console.log(results);
						res.status(200).send(
							String(results[results.length - 1])
						);
						console.log(req.timedout);
						console.log("Data Refresh Completed.");
					} catch (err) {
						console.log(err);
						res.status(500).send(
							"Sorry, there was a system error. Please refresh the page and try again.<br/>Error 500: " +
								String(err)
						);
						console.log("Error 500 console log.");
						return;
					}
				}
			}
		);
	} catch (err) {
		console.log("Catch", err);
		res.status(500).send("Server Error. Please refresh and try again.");
		console.log("500 Catch Error");
	}

	return;
});



app.get("/process_prompt", (req, res) => {
	req.socket.setTimeout(10 * 60 * 1000);

	try {
		options = { args: [req.query.prompt, req.query.llm_name] };

		pyshell.PythonShell.run(
			path.join(__dirname + "/auto_llm/process_prompt.py"),
			options,
			function (err, results) {
				if (err) {
					console.log(err);
					res.status(500).send("Error 500.");
					console.log("Error 500 console log.");
					return;
				}
				res.status(200).send(results[0]);
				console.log(req.timedout);
				console.log("process_prompt Completed.");
			}
		);
	} catch (err) {
		console.log("Catch");
		res.status(500).send("Server Error. Please refresh and try again.");
		console.log("500 Catch Error");
	}

	return;
});



initialize();

app.listen(8000, function () {
	console.log("Working on port 8000");
});

