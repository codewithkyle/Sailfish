#!/usr/bin/env node

const argv = require("yargs").argv;
const fetch = require("node-fetch");

const mac = argv?.mac || null;
if (!mac) {
    console.log("Error: --mac is a required flag.");
    process.exit(1);
}

async function ack() {
    const request = await fetch(`http://127.0.0.1:8080/${mac}`, {
        method: "POST",
    });
    if (request.ok) {
        get();
    } else {
        console.log(`Broker responded with: ${request.status}. Sleeping for 5 seconds.`);
        setTimeout(ack, 5000);
    }
}

async function get() {
    try {
        const request = await fetch(`http://127.0.0.1:8080/${mac}`);
        if (request.ok) {
            if (request.status === 200) {
                const response = await request.json();
                const data = response.data;
                console.log(response.id);
                if (data.temp <= 15) {
                    console.log("Burr. It's cold.", data.temp);
                } else if (data.temp >= 30) {
                    console.log("Damn it's hot.", data.temp);
                } else {
                    console.log("Perfect temp!", data.temp);
                }
                ack();
            } else {
                console.log("No new events. Sleeping for 5 seconds.");
                setTimeout(get, 5000);
            }
        } else {
            console.log(`Broker responded with: ${response.status}. Sleeping for 1 second.`);
            setTimeout(get, 1000);
        }
    } catch (e) {
        console.log(e);
        console.log(`Broker error. Sleeping for 5 seconds.`);
        setTimeout(get, 5000);
    }
}
get();
