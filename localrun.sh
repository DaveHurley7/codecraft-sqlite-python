#!/bin/sh
pipenv run python3 -m app.main companies.db "SELECT id, name FROM companies WHERE country = 'eritrea'"