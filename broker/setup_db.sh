#!/usr/bin/env bash
cd $(dirname "$0")
sqlite3 ./event-stream.db < db.sql