#!/usr/bin/env node

const argv = require("yargs").argv;
const fetch = require("node-fetch");

const mac = argv?.mac || null;
if (!mac) {
    console.log("Error: --mac is a required flag.");
    process.exit(1);
}

async function ack() {
    const response = await fetch(`http://127.0.0.1:8080/${mac}`, {
        method: "POST",
    });
    if (response.ok) {
        get();
    } else {
        console.log(`Broker responded with: ${response.status}. Sleeping for 1 second.`);
        setTimeout(ack, 1000);
    }
}

async function get() {
    const response = await fetch(`http://127.0.0.1:8080/${mac}`);
    if (response.ok) {
        if (response.status === 200) {
            try {
                const data = await response.json();
                if (data.temp <= 15) {
                    console.log("Burr. It's cold.", data.temp);
                } else if (data.temp >= 30) {
                    console.log("Damn it's hot.", data.temp);
                } else {
                    console.log("Perfect temp!", data.temp);
                }
                ack();
            } catch (e) {
                console.log(e);
                process.exit(1);
            }
        } else {
            console.log("No new events. Sleeping for 30 seconds.");
            setTimeout(get, 30000);
        }
    } else {
        console.log(`Broker responded with: ${response.status}. Sleeping for 1 second.`);
        setTimeout(get, 1000);
    }
}
get();
