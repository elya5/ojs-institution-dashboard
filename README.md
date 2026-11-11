---
title: OJS Institution Dashboard
emoji: 📊
colorFrom: indigo
colorTo: purple
sdk: docker
app_port: 7860
pinned: false
---

# OJS Dashboard

This dashboard shows information how researchers are using [OJS](https://pkp.sfu.ca/software/ojs/) journals.
The data comes from [OpenAlex](https://openalex.org/), [ROR](https://ror.org/) and the OJS Beacon.
All data sources are openly available.

Run the server with:
```
$ uv sync
$ uv run fastapi run server.py
```

and the frontend with:
```
$ npm install
$ npm run dev
```
